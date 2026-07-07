package internal

import (
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

const (
	DriverName               = "local-block-storage-csi"
	DriverVersion            = "0.1.0"
	TopologyKey              = "topology.local-block-storage-csi/node"
	DefaultEndpoint          = "unix:///csi/csi.sock"
	DefaultDataRoot          = "/var/lib/local-block-storage-csi"
	DefaultStateRoot         = "/var/lib/local-block-storage-csi/state"
	DefaultDeviceSymlinkRoot = "/dev/local-block-storage-csi"
)

var (
	GitCommit = "unknown"
	BuildTime = "unknown"
	GitTag    = ""
)

type Config struct {
	Endpoint           string
	NodeID             string
	DataRoot           string
	StateRoot          string
	ControllerEnabled  bool
	NodeEnabled        bool
	CleanupLoopDevices bool
	DeviceSymlinkRoot  string
}

type LoopDevice string
type BackingFile string

type Volume struct {
	d *Driver

	VolumeID      string                `json:"volumeID"`
	BackingFile   BackingFile           `json:"backingFile"`
	CapacityBytes int64                 `json:"capacityBytes"`
	NodeID        string                `json:"nodeID"`
	LoopDevice    LoopDevice            `json:"loopDevice,omitempty"`
	PublishedTo   map[string]LoopDevice `json:"publishedTo,omitempty"`
}

type Driver struct {
	cfg Config

	mu sync.Mutex

	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
}

func New(cfg Config) *Driver {
	if cfg.DeviceSymlinkRoot == "" {
		cfg.DeviceSymlinkRoot = DefaultDeviceSymlinkRoot
	}
	return &Driver{cfg: cfg}
}
