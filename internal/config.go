package internal

import (
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

const (
	DriverName                    = "local-block-storage-csi"
	DriverVersion                 = "0.1.0"
	TopologyKey                   = "topology.local-block-storage-csi/node"
	DefaultEndpoint               = "unix:///csi/csi.sock"
	DefaultDataRoot               = "/var/lib/local-block-storage-csi"
	DefaultStateRoot              = "/var/lib/local-block-storage-csi/state"
	DefaultDeviceSymlinkRoot      = "/dev/local-block-storage-csi"
	DefaultLoopCleanupMinInterval = 30 * time.Second
)

type Config struct {
	Endpoint               string
	NodeID                 string
	DataRoot               string
	StateRoot              string
	Mode                   string
	CleanupLoopDevices     bool
	DeviceSymlinkRoot      string
	LoopCleanupMinInterval time.Duration
}

type volumeState struct {
	VolumeID         string            `json:"volumeID"`
	BackingFile      string            `json:"backingFile"`
	CapacityBytes    int64             `json:"capacityBytes"`
	NodeID           string            `json:"nodeID"`
	LoopDevice       string            `json:"loopDevice,omitempty"`
	DeviceSymlink    string            `json:"deviceSymlink,omitempty"`
	StagedPath       string            `json:"stagedPath,omitempty"`
	PublishedTo      map[string]string `json:"publishedTo,omitempty"`
	StagedDevicePath string            `json:"stagedDevicePath,omitempty"`
}

type Driver struct {
	cfg Config

	mu sync.Mutex

	lastLoopCleanup time.Time

	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
}

func New(cfg Config) *Driver {
	if cfg.DeviceSymlinkRoot == "" {
		cfg.DeviceSymlinkRoot = DefaultDeviceSymlinkRoot
	}
	if cfg.LoopCleanupMinInterval <= 0 {
		cfg.LoopCleanupMinInterval = DefaultLoopCleanupMinInterval
	}
	return &Driver{cfg: cfg}
}
