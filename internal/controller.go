package internal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if d.cfg.Mode != "controller" {
		return nil, status.Error(codes.Unimplemented, "controller service not enabled")
	}
	if accessibility := req.GetAccessibilityRequirements(); accessibility != nil {
		klog.Infof("CreateVolume name=%s accessibility preferred=%d requisite=%d", req.GetName(), len(accessibility.GetPreferred()), len(accessibility.GetRequisite()))
	} else {
		klog.Infof("CreateVolume name=%s accessibility missing", req.GetName())
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing name")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing volume capabilities")
	}

	for _, volumeCapability := range req.GetVolumeCapabilities() {
		if volumeCapability.GetBlock() == nil {
			return nil, status.Error(codes.InvalidArgument, "this driver supports only raw block volumes")
		}
		if accessMode := volumeCapability.GetAccessMode(); accessMode != nil {
			switch accessMode.GetMode() {
			case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
				csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
			default:
				return nil, status.Errorf(codes.InvalidArgument, "unsupported access mode: %s", accessMode.Mode.String())
			}
		}
	}

	capBytes := int64(1 << 30)
	if capacityRange := req.GetCapacityRange(); capacityRange != nil {
		if capacityRange.RequiredBytes > 0 {
			capBytes = capacityRange.RequiredBytes
		}
		if capacityRange.LimitBytes > 0 && capBytes > capacityRange.LimitBytes {
			return nil, status.Error(codes.InvalidArgument, "required_bytes exceeds limit_bytes")
		}
	}

	nodeID, err := d.pickNodeFromAccessibility(req.GetAccessibilityRequirements())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "cannot choose target node: %v", err)
	}

	if existing, ok := d.findVolumeByName(req.GetName()); ok {
		klog.Infof("CreateVolume idempotent hit name=%s volumeID=%s", req.GetName(), existing.VolumeID)
		if existing.NodeID != nodeID {
			return nil, status.Errorf(codes.AlreadyExists, "volume %q already exists on node %q, requested node %q",
				req.GetName(), existing.NodeID, nodeID)
		}
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      existing.VolumeID,
				CapacityBytes: existing.CapacityBytes,
				AccessibleTopology: []*csi.Topology{
					{Segments: map[string]string{TopologyKey: existing.NodeID}},
				},
				VolumeContext: map[string]string{
					"backingFile": existing.BackingFile,
					"nodeID":      existing.NodeID,
				},
			},
		}, nil
	}

	volumeID := uuid.NewString()
	backingFile := filepath.Join(d.cfg.DataRoot, "volumes", volumeID+".img")
	klog.Infof("CreateVolume allocate volumeID=%s nodeID=%s backingFile=%s size=%d", volumeID, nodeID, backingFile, capBytes)

	if err := d.createSparseFile(backingFile, capBytes); err != nil {
		return nil, status.Errorf(codes.Internal, "create sparse file: %v", err)
	}

	state := &volumeState{
		VolumeID:      volumeID,
		BackingFile:   backingFile,
		CapacityBytes: capBytes,
		NodeID:        nodeID,
		PublishedTo:   map[string]string{},
	}
	if err := d.saveVolumeStateByID(state); err != nil {
		return nil, status.Errorf(codes.Internal, "persist volume state: %v", err)
	}
	if err := d.saveNameIndex(req.GetName(), volumeID); err != nil {
		return nil, status.Errorf(codes.Internal, "persist name index: %v", err)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: capBytes,
			AccessibleTopology: []*csi.Topology{
				{Segments: map[string]string{TopologyKey: nodeID}},
			},
			VolumeContext: map[string]string{
				"backingFile": backingFile,
				"nodeID":      nodeID,
			},
		},
	}, nil
}

func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if d.cfg.Mode != "controller" {
		return nil, status.Error(codes.Unimplemented, "controller service not enabled")
	}
	klog.Infof("DeleteVolume volumeID=%s", req.GetVolumeId())
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing volume_id")
	}

	state, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil {
		if os.IsNotExist(err) {
			klog.Infof("DeleteVolume volumeID=%s already deleted", req.GetVolumeId())
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "load state: %v", err)
	}

	if state.BackingFile != "" {
		d.mu.Lock()
		loopDevice, err := d.findLoopDeviceByBackingFile(state.BackingFile)
		if err != nil {
			d.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "find loop device for backing file %s: %v", state.BackingFile, err)
		}
		if loopDevice != "" {
			loopNumber, err := d.loopDeviceNumber(loopDevice)
			if err != nil {
				d.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "parse loop device %s: %v", loopDevice, err)
			}
			if err := d.losetupDeviceDetach(loopNumber); err != nil {
				d.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "detach loop device %s: %v", loopDevice, err)
			}
		}
		d.mu.Unlock()
		if err := os.Remove(state.BackingFile); err != nil && !os.IsNotExist(err) {
			return nil, status.Errorf(codes.Internal, "remove backing file %s: %v", state.BackingFile, err)
		}
	}
	_ = os.Remove(d.volumeStatePath(req.GetVolumeId()))
	_ = d.deleteNameIndexByVolumeID(req.GetVolumeId())
	if err := d.RemoveDeviceSymlink(req.GetVolumeId()); err != nil {
		klog.Warningf("DeleteVolume failed removing device symlink volumeID=%s: %v", req.GetVolumeId(), err)
	}
	klog.Infof("DeleteVolume cleaned volumeID=%s backingFile=%s", req.GetVolumeId(), state.BackingFile)

	return &csi.DeleteVolumeResponse{}, nil
}

func (d *Driver) ValidateVolumeCapabilities(_ context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing volume_id")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing volume capabilities")
	}

	for _, volumeCapability := range req.GetVolumeCapabilities() {
		if volumeCapability.GetBlock() == nil {
			return &csi.ValidateVolumeCapabilitiesResponse{
				Message: "only raw block volumes are supported",
			}, nil
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
			VolumeContext:      req.GetVolumeContext(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (d *Driver) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	if d.cfg.Mode != "controller" {
		return nil, status.Error(codes.Unimplemented, "controller service not enabled")
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			d.ctrlCap(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
		},
	}, nil
}

func (d *Driver) GetCapacity(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return &csi.GetCapacityResponse{
		AvailableCapacity: 0,
	}, nil
}

func (d *Driver) ControllerPublishVolume(context.Context, *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "attach not supported for local loop-backed volumes")
}
func (d *Driver) ControllerUnpublishVolume(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "attach not supported for local loop-backed volumes")
}
func (d *Driver) ListVolumes(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (d *Driver) CreateSnapshot(context.Context, *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (d *Driver) DeleteSnapshot(context.Context, *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (d *Driver) ListSnapshots(context.Context, *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (d *Driver) ControllerExpandVolume(context.Context, *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (d *Driver) ctrlCap(capabilityType csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
	return &csi.ControllerServiceCapability{
		Type: &csi.ControllerServiceCapability_Rpc{
			Rpc: &csi.ControllerServiceCapability_RPC{Type: capabilityType},
		},
	}
}

func (d *Driver) createSparseFile(path string, size int64) error {
	fileHandle, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer fileHandle.Close()
	return fileHandle.Truncate(size)
}

func (d *Driver) pickNodeFromAccessibility(accessibility *csi.TopologyRequirement) (string, error) {
	if accessibility == nil {
		return "", errors.New("missing accessibility requirements; use WaitForFirstConsumer and topology-aware provisioning")
	}
	for _, topology := range accessibility.Preferred {
		if value := topology.GetSegments()[TopologyKey]; value != "" {
			return value, nil
		}
	}
	for _, topology := range accessibility.Requisite {
		if value := topology.GetSegments()[TopologyKey]; value != "" {
			return value, nil
		}
	}
	return "", fmt.Errorf("topology key %q not found", TopologyKey)
}
