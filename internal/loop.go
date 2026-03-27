package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	losetup "github.com/freddierice/go-losetup/v2"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

func (d *Driver) ensureLoopDevice(backingFile string) (string, error) {
	if devicePath, err := d.findLoopDeviceByBackingFile(backingFile); err != nil {
		return "", fmt.Errorf("scan loop devices: %v", err)
	} else if devicePath != "" {
		klog.Infof("reuse loop device backingFile=%s loopDevice=%s", backingFile, devicePath)
		return devicePath, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if count, err := d.countLoopDevices(); err == nil && count == 0 {
		if err := d.ensureNextLoopDeviceNode(); err != nil {
			return "", fmt.Errorf("ensure loop device node: %w", err)
		}
	}

	const maxAttachAttempts = 10
	var lastErr error
	for attemptIndex := 0; attemptIndex < maxAttachAttempts; attemptIndex++ {
		loopDevice, err := losetup.Attach(backingFile, 0, false)
		if err == nil {
			klog.Infof("attached loop device backingFile=%s loopDevice=%s", backingFile, loopDevice.Path())
			return loopDevice.Path(), nil
		}
		lastErr = err
		if ensureErr := d.ensureNextLoopDeviceNode(); ensureErr != nil {
			break
		}
	}
	if count, countErr := d.countLoopDevices(); countErr == nil && count == 0 {
		return "", fmt.Errorf("attach loop device: %w (no /dev/loopN devices found; ensure loop module is loaded and device nodes exist)", lastErr)
	}
	return "", fmt.Errorf("attach loop device: %w", lastErr)
}

func (d *Driver) findLoopDeviceByBackingFile(backingFile string) (string, error) {
	paths, err := filepath.Glob("/dev/loop*")
	if err != nil {
		return "", err
	}
	for _, path := range paths {
		if path == "/dev/loop-control" {
			continue
		}
		loopNumber, err := d.loopDeviceNumber(path)
		if err != nil {
			continue
		}
		info, err := losetup.New(loopNumber, os.O_RDONLY).GetInfo()
		if err != nil {
			continue
		}
		fileName := strings.TrimRight(string(info.FileName[:]), "\x00")
		if fileName == backingFile {
			return losetup.New(loopNumber, os.O_RDONLY).Path(), nil
		}
	}
	return "", nil
}

func (d *Driver) loopDeviceNumber(path string) (uint64, error) {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, "loop") {
		return 0, fmt.Errorf("not a loop device path: %s", path)
	}
	numStr := strings.TrimPrefix(base, "loop")
	if numStr == "" {
		return 0, fmt.Errorf("missing loop device number: %s", path)
	}
	return strconv.ParseUint(numStr, 10, 64)
}

func (d *Driver) losetupDeviceDetach(loopNumber uint64) error {
	return losetup.New(loopNumber, os.O_RDONLY).Detach()
}

func (d *Driver) CleanupLoopDevices() error {
	knownBackingFiles, err := d.loadKnownBackingFiles()
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
		loopNumber, err := d.loopDeviceNumber(path)
		if err != nil {
			continue
		}
		info, err := losetup.New(loopNumber, os.O_RDONLY).GetInfo()
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
		if _, ok := knownBackingFiles[backingFile]; ok {
			continue
		}
		if _, err := os.Stat(backingFile); err != nil {
			klog.Warningf("detaching loop device with missing backing file: %s (%s)", path, backingFile)
			_ = losetup.New(loopNumber, os.O_RDONLY).Detach()
			continue
		}
		klog.Warningf("detaching unknown loop device: %s (%s)", path, backingFile)
		_ = losetup.New(loopNumber, os.O_RDONLY).Detach()
	}

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
		loopNumber, err := d.loopDeviceNumber(path)
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

func (d *Driver) loadKnownBackingFiles() (map[string]struct{}, error) {
	knownBackingFiles := map[string]struct{}{}
	paths, err := filepath.Glob(filepath.Join(d.cfg.StateRoot, "volumes", "*.json"))
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		bytesData, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var state volumeState
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
