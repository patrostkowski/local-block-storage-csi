package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/klog/v2"
)

func (d *Driver) volumeDeviceSymlinkPath(volumeID string) (string, error) {
	if volumeID == "" {
		return "", fmt.Errorf("missing volume ID")
	}
	if strings.Contains(volumeID, string(os.PathSeparator)) {
		return "", fmt.Errorf("invalid volume ID %q: contains path separator", volumeID)
	}
	if volumeID == "." || volumeID == ".." {
		return "", fmt.Errorf("invalid volume ID %q", volumeID)
	}
	return filepath.Join(d.cfg.DeviceSymlinkRoot, volumeID), nil
}

func (d *Driver) EnsureDeviceSymlink(volumeID, loopDevice string) (string, error) {
	if loopDevice == "" {
		return "", fmt.Errorf("missing loop device")
	}
	if !strings.HasPrefix(loopDevice, "/dev/loop") {
		return "", fmt.Errorf("invalid loop device path %q", loopDevice)
	}
	if _, err := os.Stat(loopDevice); err != nil {
		return "", fmt.Errorf("stat loop device %s: %w", loopDevice, err)
	}

	symlinkPath, err := d.volumeDeviceSymlinkPath(volumeID)
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(d.cfg.DeviceSymlinkRoot, 0o755); err != nil {
		return "", fmt.Errorf("mkdir symlink root %s: %w", d.cfg.DeviceSymlinkRoot, err)
	}

	existing, err := os.Lstat(symlinkPath)
	if err == nil {
		if (existing.Mode() & os.ModeSymlink) != 0 {
			target, readErr := os.Readlink(symlinkPath)
			if readErr == nil {
				resolved := target
				if !filepath.IsAbs(resolved) {
					resolved = filepath.Join(filepath.Dir(symlinkPath), resolved)
				}
				if resolved == loopDevice {
					if _, statErr := os.Stat(loopDevice); statErr == nil {
						return symlinkPath, nil
					}
				}
			}
		}
		if removeErr := os.Remove(symlinkPath); removeErr != nil {
			return "", fmt.Errorf("remove stale symlink %s: %w", symlinkPath, removeErr)
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("lstat symlink path %s: %w", symlinkPath, err)
	}

	if err := os.Symlink(loopDevice, symlinkPath); err != nil {
		return "", fmt.Errorf("create symlink %s -> %s: %w", symlinkPath, loopDevice, err)
	}
	klog.Infof("updated volume symlink volumeID=%s symlink=%s loopDevice=%s", volumeID, symlinkPath, loopDevice)
	return symlinkPath, nil
}

func (d *Driver) RemoveDeviceSymlink(volumeID string) error {
	symlinkPath, err := d.volumeDeviceSymlinkPath(volumeID)
	if err != nil {
		return err
	}
	if err := os.Remove(symlinkPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove symlink %s: %w", symlinkPath, err)
	}
	return nil
}

func (d *Driver) ResolveVolumeDevice(volumeID string) (string, error) {
	symlinkPath, err := d.volumeDeviceSymlinkPath(volumeID)
	if err != nil {
		return "", err
	}

	target, readlinkErr := os.Readlink(symlinkPath)
	if readlinkErr == nil {
		resolved := target
		if !filepath.IsAbs(resolved) {
			resolved = filepath.Join(filepath.Dir(symlinkPath), resolved)
		}
		if _, statErr := os.Stat(resolved); statErr == nil {
			return resolved, nil
		}
	}

	state, stateErr := d.loadVolumeStateByID(volumeID)
	if stateErr != nil {
		if readlinkErr != nil {
			return "", fmt.Errorf("read volume symlink %s: %w", symlinkPath, readlinkErr)
		}
		return "", fmt.Errorf("load volume state %s: %w", volumeID, stateErr)
	}

	if state.LoopDevice != "" {
		if _, statErr := os.Stat(state.LoopDevice); statErr == nil {
			if _, ensureErr := d.EnsureDeviceSymlink(volumeID, state.LoopDevice); ensureErr != nil {
				klog.Warningf("failed to refresh volume symlink volumeID=%s loopDevice=%s: %v", volumeID, state.LoopDevice, ensureErr)
			}
			return state.LoopDevice, nil
		}
	}

	if state.BackingFile == "" {
		return "", fmt.Errorf("volume %s has no backing file", volumeID)
	}

	loopDevice, findErr := d.findLoopDeviceByBackingFile(state.BackingFile)
	if findErr != nil {
		return "", fmt.Errorf("find loop device for %s: %w", state.BackingFile, findErr)
	}
	if loopDevice == "" {
		return "", fmt.Errorf("no loop device found for volume %s backing file %s", volumeID, state.BackingFile)
	}

	if _, ensureErr := d.EnsureDeviceSymlink(volumeID, loopDevice); ensureErr != nil {
		klog.Warningf("failed to refresh volume symlink volumeID=%s loopDevice=%s: %v", volumeID, loopDevice, ensureErr)
	}
	return loopDevice, nil
}

func (d *Driver) cleanupStaleVolumeSymlinksLocked(knownVolumeIDs map[string]struct{}) {
	entries, err := os.ReadDir(d.cfg.DeviceSymlinkRoot)
	if err != nil {
		if !os.IsNotExist(err) {
			klog.Warningf("list symlink root %s: %v", d.cfg.DeviceSymlinkRoot, err)
		}
		return
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			continue
		}
		symlinkPath := filepath.Join(d.cfg.DeviceSymlinkRoot, name)

		_, known := knownVolumeIDs[name]
		if !known {
			if err := os.Remove(symlinkPath); err != nil && !os.IsNotExist(err) {
				klog.Warningf("remove stale symlink %s: %v", symlinkPath, err)
			}
			continue
		}

		target, readErr := os.Readlink(symlinkPath)
		if readErr != nil {
			if err := os.Remove(symlinkPath); err != nil && !os.IsNotExist(err) {
				klog.Warningf("remove invalid symlink %s: %v", symlinkPath, err)
			}
			continue
		}

		resolved := target
		if !filepath.IsAbs(resolved) {
			resolved = filepath.Join(filepath.Dir(symlinkPath), resolved)
		}
		if _, statErr := os.Stat(resolved); statErr != nil {
			if err := os.Remove(symlinkPath); err != nil && !os.IsNotExist(err) {
				klog.Warningf("remove broken symlink %s: %v", symlinkPath, err)
			}
		}
	}
}

func (d *Driver) reconcileVolumeSymlinks() {
	paths, err := filepath.Glob(filepath.Join(d.cfg.StateRoot, "volumes", "*.json"))
	if err != nil {
		klog.Warningf("list volume states for symlink reconciliation: %v", err)
		return
	}

	for _, path := range paths {
		bytesData, readErr := os.ReadFile(path)
		if readErr != nil {
			continue
		}
		var state volumeState
		if unmarshalErr := json.Unmarshal(bytesData, &state); unmarshalErr != nil {
			continue
		}
		if state.NodeID != "" && d.cfg.NodeID != "" && state.NodeID != d.cfg.NodeID {
			continue
		}
		if state.VolumeID == "" || state.BackingFile == "" {
			continue
		}

		loopDevice := state.LoopDevice
		if loopDevice == "" {
			loopDevice, err = d.findLoopDeviceByBackingFile(state.BackingFile)
			if err != nil {
				klog.Warningf("lookup loop device during symlink reconciliation volumeID=%s backingFile=%s: %v", state.VolumeID, state.BackingFile, err)
				continue
			}
		}
		if loopDevice == "" {
			continue
		}

		if _, ensureErr := d.EnsureDeviceSymlink(state.VolumeID, loopDevice); ensureErr != nil {
			klog.Warningf("ensure volume symlink during startup reconciliation volumeID=%s loopDevice=%s: %v", state.VolumeID, loopDevice, ensureErr)
		}
	}
}
