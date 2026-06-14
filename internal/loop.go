package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	losetup "github.com/freddierice/go-losetup/v2"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

func (d BackingFile) Path() string { return string(d) }

func (d BackingFile) DevInode() (uint64, uint64, error) {
	var stat unix.Stat_t
	if err := unix.Stat(d.Path(), &stat); err != nil {
		return 0, 0, err
	}
	return stat.Dev, stat.Ino, nil
}

func (d LoopDevice) Path() string { return string(d) }

func (d LoopDevice) Number() (uint64, error) {
	base := filepath.Base(d.Path())
	if !strings.HasPrefix(base, "loop") {
		return 0, fmt.Errorf("not a loop device path: %s", d.Path())
	}
	numStr := strings.TrimPrefix(base, "loop")
	if numStr == "" {
		return 0, fmt.Errorf("missing loop device number: %s", d.Path())
	}
	return strconv.ParseUint(numStr, 10, 64)
}

func (d LoopDevice) Info() (*losetup.Info, error) {
	n, err := d.Number()
	if err != nil {
		return nil, err
	}
	info, err := losetup.New(n, os.O_RDONLY).GetInfo()
	if err != nil {
		return nil, err
	}
	return &info, nil
}

func (d LoopDevice) Detach() error {
	n, err := d.Number()
	if err != nil {
		return Addf(ErrInvalid, "parse loop device %s", d.Path())
	}
	detachErr := losetup.New(n, os.O_RDONLY).Detach()
	if detachErr != nil {
		if errors.Is(detachErr, unix.EBUSY) {
			return ErrBusy
		}
		return Addf(detachErr, "detach %s", d.Path())
	}
	return nil
}

func (d *Driver) findLoopDeviceByBackingFile(backingFile BackingFile) (LoopDevice, error) {
	dev, ino, err := backingFile.DevInode()
	if err != nil {
		return "", nil
	}

	paths, err := filepath.Glob("/dev/loop*")
	if err != nil {
		return "", err
	}
	for _, path := range paths {
		if path == "/dev/loop-control" {
			continue
		}
		ld := LoopDevice(path)
		info, err := ld.Info()
		if err != nil {
			continue
		}
		if info.Device == dev && info.INode == ino {
			return ld, nil
		}
	}
	return "", nil
}

func (d *Driver) CleanupLoopDevices() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.cleanupStaleLoopDevicesLocked()
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (d *Driver) cleanupStaleLoopDevicesLocked() error {
	knownBackingFiles, err := d.loadKnownBackingFiles()
	if err != nil {
		return err
	}
	knownVolumeIDs, err := d.loadKnownVolumeIDs()
	if err != nil {
		return err
	}

	paths, err := filepath.Glob("/dev/loop*")
	if err != nil {
		return err
	}
	for _, path := range paths {
		if path == "/dev/loop-control" {
			continue
		}
		loopDevice := LoopDevice(path)
		info, err := loopDevice.Info()
		if err != nil {
			continue
		}
		backingFile := strings.TrimRight(string(info.FileName[:]), "\x00")
		if backingFile == "" {
			continue
		}
		if !strings.HasPrefix(backingFile, d.cfg.DataRoot+string(os.PathSeparator)) {
			continue
		}
		if _, ok := knownBackingFiles[BackingFile(backingFile)]; ok {
			continue
		}

		if _, err := os.Stat(backingFile); err != nil {
			if os.IsNotExist(err) {
				klog.Warningf("detaching stale loop device: path=%s backingFile=%s reason=missing_backing_file", path, backingFile)
				if detachErr := loopDevice.Detach(); detachErr != nil && !errors.Is(detachErr, ErrBusy) {
					klog.Warningf("failed detaching stale loop device path=%s backingFile=%s: %v", path, backingFile, detachErr)
				}
			}
			continue
		}

		klog.V(2).Infof("keeping loop device with existing unknown backing file path=%s backingFile=%s", path, backingFile)
	}

	d.cleanupStaleVolumeSymlinksLocked(knownVolumeIDs)

	return nil
}

func (d *Driver) countLoopDevices() (int, error) {
	paths, err := filepath.Glob("/dev/loop[0-9]*")
	if err != nil {
		return 0, err
	}
	return len(paths), nil
}

func (d *Driver) ensureNextLoopDeviceNode() error {
	if _, err := os.Stat("/dev/loop-control"); err != nil {
		return fmt.Errorf("missing /dev/loop-control")
	}
	paths, err := filepath.Glob("/dev/loop[0-9]*")
	if err != nil {
		return err
	}
	used := map[uint64]struct{}{}
	max := uint64(0)
	for _, path := range paths {
		loopNumber, err := LoopDevice(path).Number()
		if err != nil {
			continue
		}
		used[loopNumber] = struct{}{}
		if loopNumber > max {
			max = loopNumber
		}
	}
	var next uint64
	for loopIndex := uint64(0); loopIndex <= max; loopIndex++ {
		if _, ok := used[loopIndex]; !ok {
			next = loopIndex
			break
		}
		next = max + 1
	}
	path := fmt.Sprintf("/dev/loop%d", next)
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	mode := uint32(unix.S_IFBLK | 0o660)
	dev := unix.Mkdev(7, uint32(next))
	if err := unix.Mknod(path, mode, int(dev)); err != nil {
		if os.IsExist(err) {
			return nil
		}
		return err
	}
	klog.Infof("created loop device node path=%s major=7 minor=%d", path, next)
	return os.Chown(path, 0, 0)
}

func (d *Driver) loadKnownBackingFiles() (map[BackingFile]struct{}, error) {
	knownBackingFiles := map[BackingFile]struct{}{}
	paths, err := filepath.Glob(filepath.Join(d.cfg.StateRoot, "volumes", "*.json"))
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		bytesData, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var state Volume
		if err := json.Unmarshal(bytesData, &state); err != nil {
			continue
		}
		if d.cfg.NodeID != "" && state.NodeID != d.cfg.NodeID {
			continue
		}
		if state.BackingFile != "" {
			knownBackingFiles[state.BackingFile] = struct{}{}
		}
	}
	return knownBackingFiles, nil
}

func (d *Driver) loadKnownVolumeIDs() (map[string]struct{}, error) {
	knownVolumeIDs := map[string]struct{}{}
	paths, err := filepath.Glob(filepath.Join(d.cfg.StateRoot, "volumes", "*.json"))
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		base := filepath.Base(path)
		if !strings.HasSuffix(base, ".json") {
			continue
		}
		volumeID := strings.TrimSuffix(base, ".json")
		if volumeID == "" {
			continue
		}
		knownVolumeIDs[volumeID] = struct{}{}
	}
	return knownVolumeIDs, nil
}
