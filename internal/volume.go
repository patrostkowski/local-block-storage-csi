package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	losetup "github.com/freddierice/go-losetup/v2"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

func (v *Volume) Load() error {
	bytesData, err := os.ReadFile(v.d.volumeStatePath(v.VolumeID))
	if err != nil {
		return err
	}
	var loaded Volume
	if err := json.Unmarshal(bytesData, &loaded); err != nil {
		return err
	}
	*v = loaded
	v.d = loaded.d
	return nil
}

func (v *Volume) Save() error {
	temporaryPath := v.d.volumeStatePath(v.VolumeID) + ".tmp"
	bytesData, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(temporaryPath, bytesData, 0o600); err != nil {
		return err
	}
	return os.Rename(temporaryPath, v.d.volumeStatePath(v.VolumeID))
}

func (v *Volume) EnsureLoopDevice() error {
	if v.LoopDevice != "" {
		return nil
	}

	devicePath, err := v.d.findLoopDeviceByBackingFile(v.BackingFile)
	if err != nil {
		return fmt.Errorf("scan loop devices: %w", err)
	}
	if devicePath != "" {
		klog.Infof("reuse loop device backingFile=%s loopDevice=%s", v.BackingFile.Path(), devicePath.Path())
		v.LoopDevice = devicePath
		return nil
	}

	if count, err := v.d.countLoopDevices(); err == nil && count == 0 {
		if err := v.d.ensureNextLoopDeviceNode(); err != nil {
			return fmt.Errorf("ensure loop device node: %w", err)
		}
	}

	const maxAttachAttempts = 10
	var lastErr error
	for range maxAttachAttempts {
		loopDevice, err2 := losetup.Attach(v.BackingFile.Path(), 0, false)
		if err2 == nil {
			klog.Infof("attached loop device backingFile=%s loopDevice=%s", v.BackingFile.Path(), loopDevice.Path())
			v.LoopDevice = LoopDevice(loopDevice.Path())
			_ = v.d.ensureNextLoopDeviceNode()
			return nil
		}
		lastErr = err2
		if ensureErr := v.d.ensureNextLoopDeviceNode(); ensureErr != nil {
			break
		}
	}
	if count, countErr := v.d.countLoopDevices(); countErr == nil && count == 0 {
		return fmt.Errorf("attach loop device: %w (no /dev/loopN devices found; ensure loop module is loaded and device nodes exist)", lastErr)
	}
	return fmt.Errorf("attach loop device: %w", lastErr)
}

func (v *Volume) DetachLoopDevice() error {
	if v.LoopDevice == "" {
		return nil
	}
	if err := v.LoopDevice.Detach(); err != nil {
		if errors.Is(err, ErrBusy) {
			return nil
		}
		return err
	}
	v.LoopDevice = ""
	return nil
}

func (v *Volume) Symlink() error {
	if v.LoopDevice == "" {
		return fmt.Errorf("no loop device set for volume %s", v.VolumeID)
	}
	_, err := v.d.EnsureDeviceSymlink(v.VolumeID, v.LoopDevice.Path())
	return err
}

func (v *Volume) RemoveSymlink() error {
	return v.d.RemoveDeviceSymlink(v.VolumeID)
}

func (v *Volume) Publish(targetPath string) error {
	if v.LoopDevice == "" {
		return fmt.Errorf("no loop device set for volume %s", v.VolumeID)
	}

	srcInfo, err := os.Stat(v.LoopDevice.Path())
	if err != nil {
		return fmt.Errorf("stat source device %s: %w", v.LoopDevice.Path(), err)
	}
	srcStat, ok := srcInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unexpected stat type for %s", v.LoopDevice.Path())
	}
	if (srcInfo.Mode() & os.ModeDevice) == 0 {
		return fmt.Errorf("source is not a device: %s", v.LoopDevice.Path())
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("mkdir parent: %w", err)
	}

	if st, statErr := os.Lstat(targetPath); statErr == nil {
		if (st.Mode() & os.ModeDevice) != 0 {
			dstStat, ok2 := st.Sys().(*syscall.Stat_t)
			if !ok2 {
				return fmt.Errorf("unexpected stat type for %s", targetPath)
			}
			if unix.Major(uint64(dstStat.Rdev)) == unix.Major(uint64(srcStat.Rdev)) &&
				unix.Minor(uint64(dstStat.Rdev)) == unix.Minor(uint64(srcStat.Rdev)) {
				return nil
			}
		}
		if !v.d.isMounted(targetPath) {
			if rmErr := os.Remove(targetPath); rmErr != nil {
				return fmt.Errorf("remove stale target %s: %w", targetPath, rmErr)
			}
		}
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("lstat target %s: %w", targetPath, statErr)
	}

	f, createErr := os.OpenFile(targetPath, os.O_CREATE, 0o660)
	if createErr != nil {
		return fmt.Errorf("create target file %s: %w", targetPath, createErr)
	}
	f.Close()

	if v.d.isMounted(targetPath) {
		return nil
	}

	if mountErr := unix.Mount(v.LoopDevice.Path(), targetPath, "", unix.MS_BIND, ""); mountErr != nil {
		return fmt.Errorf("bind mount %s -> %s: %w", v.LoopDevice.Path(), targetPath, mountErr)
	}

	if v.PublishedTo == nil {
		v.PublishedTo = map[string]LoopDevice{}
	}
	v.PublishedTo[targetPath] = v.LoopDevice

	return nil
}

func (v *Volume) Unpublish(targetPath string) error {
	if v.d.isMounted(targetPath) {
		if unmountErr := unix.Unmount(targetPath, 0); unmountErr != nil {
			return fmt.Errorf("unmount target %s: %w", targetPath, unmountErr)
		}
	}
	if err := removeIfExists(targetPath); err != nil {
		return fmt.Errorf("remove target %s: %w", targetPath, err)
	}
	if v.PublishedTo != nil {
		delete(v.PublishedTo, targetPath)
		for tp := range v.PublishedTo {
			if _, statErr := os.Stat(tp); statErr != nil && os.IsNotExist(statErr) {
				delete(v.PublishedTo, tp)
			}
		}
	}
	return nil
}

func (v *Volume) Delete() error {
	v.DetachLoopDevice()
	v.RemoveSymlink()
	if v.BackingFile != "" {
		removeIfExists(v.BackingFile.Path())
	}
	removeIfExists(v.d.volumeStatePath(v.VolumeID))
	return nil
}
