package internal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

func (d *Driver) volumeStatePath(volumeID string) string {
	return filepath.Join(d.cfg.StateRoot, "volumes", volumeID+".json")
}

func (d *Driver) nameIndexPath(name string) string {
	safe := strings.ReplaceAll(name, "/", "_")
	return filepath.Join(d.cfg.StateRoot, "volumes", "name-"+safe+".idx")
}

func (d *Driver) saveNameIndex(name, volumeID string) error {
	return os.WriteFile(d.nameIndexPath(name), []byte(volumeID), 0o600)
}

func (d *Driver) findVolumeByName(name string) (*volumeState, bool) {
	bytesData, err := os.ReadFile(d.nameIndexPath(name))
	if err != nil {
		return nil, false
	}
	volumeID := strings.TrimSpace(string(bytesData))
	state, err := d.loadVolumeStateByID(volumeID)
	if err != nil {
		return nil, false
	}
	return state, true
}

func (d *Driver) deleteNameIndexByVolumeID(volumeID string) error {
	paths, err := filepath.Glob(filepath.Join(d.cfg.StateRoot, "volumes", "name-*.idx"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		bytesData, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(bytesData)) == volumeID {
			_ = os.Remove(path)
		}
	}
	return nil
}

func (d *Driver) saveVolumeStateByID(state *volumeState) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	temporaryPath := d.volumeStatePath(state.VolumeID) + ".tmp"
	bytesData, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(temporaryPath, bytesData, 0o600); err != nil {
		return err
	}
	return os.Rename(temporaryPath, d.volumeStatePath(state.VolumeID))
}

func (d *Driver) loadVolumeStateByID(volumeID string) (*volumeState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	bytesData, err := os.ReadFile(d.volumeStatePath(volumeID))
	if err != nil {
		return nil, err
	}
	var state volumeState
	if err := json.Unmarshal(bytesData, &state); err != nil {
		return nil, err
	}
	return &state, nil
}
