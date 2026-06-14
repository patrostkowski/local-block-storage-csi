package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		if existing.CapacityBytes != capBytes {
			return nil, status.Errorf(codes.AlreadyExists, "volume %q already exists with capacity %d, requested %d",
				req.GetName(), existing.CapacityBytes, capBytes)
		}
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      existing.VolumeID,
				CapacityBytes: existing.CapacityBytes,
				AccessibleTopology: []*csi.Topology{
					{Segments: map[string]string{TopologyKey: existing.NodeID}},
				},
				VolumeContext: map[string]string{
					"backingFile": existing.BackingFile.Path(),
					"nodeID":      existing.NodeID,
				},
			},
		}, nil
	}

	volumeID := uuid.NewString()
	backingFile := filepath.Join(d.cfg.DataRoot, "volumes", volumeID+".img")
	klog.Infof("CreateVolume allocate volumeID=%s nodeID=%s backingFile=%s size=%d", volumeID, nodeID, backingFile, capBytes)

	var contentSource *csi.VolumeContentSource
	if contentSrc := req.GetVolumeContentSource(); contentSrc != nil {
		if srcVol := contentSrc.GetVolume(); srcVol != nil {
			src, loadErr := d.loadVolumeStateByID(srcVol.GetVolumeId())
			if loadErr != nil {
				return nil, status.Errorf(codes.NotFound, "source volume not found: %v", loadErr)
			}
			if src.NodeID != nodeID {
				return nil, status.Error(codes.FailedPrecondition, "source volume must be on the same node")
			}
			if err := d.copyFile(backingFile, src.BackingFile.Path(), capBytes); err != nil {
				return nil, status.Errorf(codes.Internal, "clone backing file: %v", err)
			}
			contentSource = &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Volume{
					Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: src.VolumeID},
				},
			}
			klog.Infof("CreateVolume cloned volumeID=%s from source=%s", volumeID, src.VolumeID)
		} else if srcSnap := contentSrc.GetSnapshot(); srcSnap != nil {
			snap, loadErr := d.loadSnapshot(srcSnap.GetSnapshotId())
			if loadErr != nil {
				return nil, status.Errorf(codes.NotFound, "source snapshot not found: %v", loadErr)
			}
			if err := d.copyFile(backingFile, snap.BackingFile, capBytes); err != nil {
				return nil, status.Errorf(codes.Internal, "clone snapshot: %v", err)
			}
			contentSource = &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snap.SnapshotID},
				},
			}
			klog.Infof("CreateVolume restored volumeID=%s from snapshot=%s", volumeID, snap.SnapshotID)
		}
	} else {
		if err := d.createSparseFile(backingFile, capBytes); err != nil {
			return nil, status.Errorf(codes.Internal, "create sparse file: %v", err)
		}
	}

	vol := &Volume{
		d:             d,
		VolumeID:      volumeID,
		BackingFile:   BackingFile(backingFile),
		CapacityBytes: capBytes,
		NodeID:        nodeID,
		PublishedTo:   map[string]LoopDevice{},
	}
	if err := vol.Save(); err != nil {
		return nil, status.Errorf(codes.Internal, "persist volume state: %v", err)
	}
	if err := d.saveNameIndex(req.GetName(), volumeID); err != nil {
		return nil, status.Errorf(codes.Internal, "persist name index: %v", err)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: capBytes,
			ContentSource: contentSource,
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

	vol, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil {
		if os.IsNotExist(err) {
			klog.Infof("DeleteVolume volumeID=%s already deleted", req.GetVolumeId())
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "load state: %v", err)
	}

	vol.Delete()
	_ = d.deleteNameIndexByVolumeID(req.GetVolumeId())
	klog.Infof("DeleteVolume cleaned volumeID=%s backingFile=%s", req.GetVolumeId(), vol.BackingFile)

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
			d.ctrlCap(csi.ControllerServiceCapability_RPC_LIST_VOLUMES),
			d.ctrlCap(csi.ControllerServiceCapability_RPC_EXPAND_VOLUME),
			d.ctrlCap(csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT),
			d.ctrlCap(csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS),
			d.ctrlCap(csi.ControllerServiceCapability_RPC_CLONE_VOLUME),
		},
	}, nil
}

func (d *Driver) GetCapacity(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(d.cfg.DataRoot, &stat); err != nil {
		return &csi.GetCapacityResponse{AvailableCapacity: 0}, nil
	}
	return &csi.GetCapacityResponse{
		AvailableCapacity: int64(stat.Bavail) * int64(stat.Frsize),
	}, nil
}

func (d *Driver) ControllerPublishVolume(
	ctx context.Context,
	req *csi.ControllerPublishVolumeRequest,
) (*csi.ControllerPublishVolumeResponse, error) {
	if d.cfg.Mode != "controller" {
		return nil, status.Error(codes.Unimplemented, "controller service not enabled")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing volume_id")
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing node_id")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "missing volume_capability")
	}

	state, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "load volume state: %v", err)
	}
	if state.NodeID != req.GetNodeId() {
		return nil, status.Errorf(codes.FailedPrecondition, "volume belongs to node %q, requested node %q", state.NodeID, req.GetNodeId())
	}

	// no real attach needed; confirm placement and return success
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{},
	}, nil
}

func (d *Driver) ControllerUnpublishVolume(
	ctx context.Context,
	req *csi.ControllerUnpublishVolumeRequest,
) (*csi.ControllerUnpublishVolumeResponse, error) {
	if d.cfg.Mode != "controller" {
		return nil, status.Error(codes.Unimplemented, "controller service not enabled")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing volume_id")
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (d *Driver) ListVolumes(_ context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	paths, err := filepath.Glob(filepath.Join(d.cfg.StateRoot, "volumes", "*.json"))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list volume state files: %v", err)
	}
	maxEntries := int(req.GetMaxEntries())
	if maxEntries <= 0 || maxEntries > len(paths) {
		maxEntries = len(paths)
	}
	startToken := req.GetStartingToken()
	entries := make([]*csi.ListVolumesResponse_Entry, 0, maxEntries)
	var nextToken string
	count, started := 0, startToken == ""
	for _, path := range paths {
		volumeID := strings.TrimSuffix(filepath.Base(path), ".json")
		if !started {
			if volumeID == startToken {
				started = true
			}
			continue
		}
		if count >= maxEntries {
			nextToken = volumeID
			break
		}
		vol, loadErr := d.loadVolumeStateByID(volumeID)
		if loadErr != nil {
			continue
		}
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{VolumeId: vol.VolumeID, CapacityBytes: vol.CapacityBytes},
			Status: &csi.ListVolumesResponse_VolumeStatus{},
		})
		count++
	}
	return &csi.ListVolumesResponse{Entries: entries, NextToken: nextToken}, nil
}

func (d *Driver) ControllerGetVolume(_ context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	vol, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
	}
	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      vol.VolumeID,
			CapacityBytes: vol.CapacityBytes,
			AccessibleTopology: []*csi.Topology{
				{Segments: map[string]string{TopologyKey: vol.NodeID}},
			},
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{},
	}, nil
}

func (d *Driver) CreateSnapshot(_ context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if d.cfg.Mode != "controller" {
		return nil, status.Error(codes.Unimplemented, "controller service not enabled")
	}
	if req.GetSourceVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing source_volume_id")
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing name")
	}

	src, err := d.loadVolumeStateByID(req.GetSourceVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "source volume not found: %v", err)
	}

	snapshotID := uuid.NewString()
	snapshotFile := filepath.Join(d.cfg.DataRoot, "snapshots", snapshotID+".img")

	srcFile, openErr := os.Open(src.BackingFile.Path())
	if openErr != nil {
		return nil, status.Errorf(codes.Internal, "open source backing file: %v", openErr)
	}
	defer srcFile.Close()

	dstFile, createErr := os.Create(snapshotFile)
	if createErr != nil {
		return nil, status.Errorf(codes.Internal, "create snapshot file: %v", createErr)
	}
	defer dstFile.Close()

	if _, copyErr := io.Copy(dstFile, srcFile); copyErr != nil {
		_ = os.Remove(snapshotFile)
		return nil, status.Errorf(codes.Internal, "copy snapshot: %v", copyErr)
	}

	snap := &Snapshot{
		SnapshotID:   snapshotID,
		SourceVolume: req.GetSourceVolumeId(),
		BackingFile:  snapshotFile,
		SizeBytes:    src.CapacityBytes,
		CreatedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	if saveErr := d.saveSnapshot(snap); saveErr != nil {
		_ = os.Remove(snapshotFile)
		return nil, status.Errorf(codes.Internal, "persist snapshot: %v", saveErr)
	}

	klog.Infof("CreateSnapshot snapshotID=%s sourceVolume=%s size=%d", snapshotID, src.VolumeID, src.CapacityBytes)

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshotID,
			SourceVolumeId: req.GetSourceVolumeId(),
			SizeBytes:      src.CapacityBytes,
			CreationTime:   timestamppb.New(time.Now().UTC()),
			ReadyToUse:     true,
		},
	}, nil
}

func (d *Driver) DeleteSnapshot(_ context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if d.cfg.Mode != "controller" {
		return nil, status.Error(codes.Unimplemented, "controller service not enabled")
	}
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing snapshot_id")
	}
	snap, err := d.loadSnapshot(req.GetSnapshotId())
	if err != nil {
		if os.IsNotExist(err) {
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "load snapshot: %v", err)
	}
	_ = removeIfExists(snap.BackingFile)
	_ = removeIfExists(d.snapshotStatePath(req.GetSnapshotId()))
	klog.Infof("DeleteSnapshot snapshotID=%s", req.GetSnapshotId())
	return &csi.DeleteSnapshotResponse{}, nil
}

func (d *Driver) ListSnapshots(_ context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	if d.cfg.Mode != "controller" {
		return nil, status.Error(codes.Unimplemented, "controller service not enabled")
	}
	paths, err := filepath.Glob(filepath.Join(d.cfg.StateRoot, "snapshots", "*.json"))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list snapshot state files: %v", err)
	}
	maxEntries := int(req.GetMaxEntries())
	if maxEntries <= 0 || maxEntries > len(paths) {
		maxEntries = len(paths)
	}
	startToken := req.GetStartingToken()
	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, maxEntries)
	var nextToken string
	count, started := 0, startToken == ""
	for _, path := range paths {
		snapshotID := strings.TrimSuffix(filepath.Base(path), ".json")
		if !started {
			if snapshotID == startToken {
				started = true
			}
			continue
		}
		if count >= maxEntries {
			nextToken = snapshotID
			break
		}
		snap, loadErr := d.loadSnapshot(snapshotID)
		if loadErr != nil {
			continue
		}
		if req.GetSourceVolumeId() != "" && snap.SourceVolume != req.GetSourceVolumeId() {
			continue
		}
		createdAt, _ := time.Parse(time.RFC3339, snap.CreatedAt)
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     snap.SnapshotID,
				SourceVolumeId: snap.SourceVolume,
				SizeBytes:      snap.SizeBytes,
				CreationTime:   timestamppb.New(createdAt),
				ReadyToUse:     true,
			},
		})
		count++
	}
	return &csi.ListSnapshotsResponse{Entries: entries, NextToken: nextToken}, nil
}
func (d *Driver) ControllerExpandVolume(_ context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if d.cfg.Mode != "controller" {
		return nil, status.Error(codes.Unimplemented, "controller service not enabled")
	}
	vol, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: %v", err)
	}
	newSize := req.GetCapacityRange().GetRequiredBytes()
	if newSize <= vol.CapacityBytes {
		return &csi.ControllerExpandVolumeResponse{CapacityBytes: vol.CapacityBytes, NodeExpansionRequired: false}, nil
	}
	if err := os.Truncate(vol.BackingFile.Path(), newSize); err != nil {
		return nil, status.Errorf(codes.Internal, "truncate backing file: %v", err)
	}
	vol.CapacityBytes = newSize
	if saveErr := vol.Save(); saveErr != nil {
		return nil, status.Errorf(codes.Internal, "save volume state: %v", saveErr)
	}
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         newSize,
		NodeExpansionRequired: vol.LoopDevice != "",
	}, nil
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

func (d *Driver) copyFile(dst, srcPath string, size int64) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer srcFile.Close()
	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer dstFile.Close()
	if _, copyErr := io.Copy(dstFile, srcFile); copyErr != nil {
		_ = os.Remove(dst)
		return fmt.Errorf("copy: %w", copyErr)
	}
	if truncateErr := dstFile.Truncate(size); truncateErr != nil {
		return fmt.Errorf("resize: %w", truncateErr)
	}
	return nil
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
