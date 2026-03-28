package internal

import (
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

const (
	DriverName       = "local-block-storage-csi"
	DriverVersion    = "0.1.0"
	TopologyKey      = "topology.local-block-storage-csi/node"
	DefaultEndpoint  = "unix:///csi/csi.sock"
	DefaultDataRoot  = "/var/lib/local-block-storage-csi"
	DefaultStateRoot = "/var/lib/local-block-storage-csi/state"
)

type Config struct {
	Endpoint           string
	NodeID             string
	DataRoot           string
	StateRoot          string
	Mode               string
	CleanupLoopDevices bool
}

type volumeState struct {
	VolumeID         string            `json:"volumeID"`
	BackingFile      string            `json:"backingFile"`
	CapacityBytes    int64             `json:"capacityBytes"`
	NodeID           string            `json:"nodeID"`
	LoopDevice       string            `json:"loopDevice,omitempty"`
	StagedPath       string            `json:"stagedPath,omitempty"`
	PublishedTo      map[string]string `json:"publishedTo,omitempty"`
	StagedDevicePath string            `json:"stagedDevicePath,omitempty"`
}

type Driver struct {
	cfg Config

	mu sync.Mutex

	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
}

func New(cfg Config) *Driver {
	return &Driver{cfg: cfg}
}
