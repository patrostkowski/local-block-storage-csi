package internal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (d *Driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	if d.cfg.Mode != "node" {
		return nil, status.Error(codes.Unimplemented, "node service not enabled")
	}

	return &csi.NodeGetInfoResponse{
		NodeId: d.cfg.NodeID,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{TopologyKey: d.cfg.NodeID},
		},
	}, nil
}

func (d *Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	if d.cfg.Mode != "node" {
		return nil, status.Error(codes.Unimplemented, "node service not enabled")
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			d.nodeCap(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME),
		},
	}, nil
}

func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if d.cfg.Mode != "node" {
		return nil, status.Error(codes.Unimplemented, "node service not enabled")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing volume_id")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing staging_target_path")
	}
	if req.GetVolumeCapability() == nil || req.GetVolumeCapability().GetBlock() == nil {
		return nil, status.Error(codes.InvalidArgument, "raw block capability required")
	}

	state, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "load volume state: %v", err)
	}
	if state.NodeID != d.cfg.NodeID {
		return nil, status.Errorf(codes.FailedPrecondition, "volume belongs to node %q, this node is %q", state.NodeID, d.cfg.NodeID)
	}

	if state.BackingFile == "" {
		return nil, status.Error(codes.FailedPrecondition, "backing file is empty")
	}
	info, err := os.Stat(state.BackingFile)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "backing file missing: %v", err)
	}
	if info.IsDir() {
		return nil, status.Errorf(codes.Internal, "backing file is a directory: %s", state.BackingFile)
	}

	state.StagedPath = req.GetStagingTargetPath()
	state.StagedDevicePath = ""

	if err := d.saveVolumeStateByID(state); err != nil {
		return nil, status.Errorf(codes.Internal, "save volume state: %v", err)
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if d.cfg.Mode != "node" {
		return nil, status.Error(codes.Unimplemented, "node service not enabled")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing volume_id")
	}

	state, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil {
		if os.IsNotExist(err) {
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "load volume state: %v", err)
	}

	if state.PublishedTo != nil {
		for targetPath := range state.PublishedTo {
			if _, err := os.Stat(targetPath); err != nil && os.IsNotExist(err) {
				delete(state.PublishedTo, targetPath)
			}
		}
	}

	state.StagedPath = ""
	state.StagedDevicePath = ""

	if err := d.saveVolumeStateByID(state); err != nil {
		return nil, status.Errorf(codes.Internal, "save volume state: %v", err)
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if d.cfg.Mode != "node" {
		return nil, status.Error(codes.Unimplemented, "node service not enabled")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing volume_id")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target_path")
	}
	if req.GetVolumeCapability() == nil || req.GetVolumeCapability().GetBlock() == nil {
		return nil, status.Error(codes.InvalidArgument, "raw block capability required")
	}

	state, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "load volume state: %v", err)
	}
	if state.NodeID != d.cfg.NodeID {
		return nil, status.Errorf(codes.FailedPrecondition, "volume belongs to node %q, this node is %q", state.NodeID, d.cfg.NodeID)
	}
	if state.BackingFile == "" {
		return nil, status.Error(codes.FailedPrecondition, "backing file is empty")
	}

	info, err := os.Stat(state.BackingFile)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stat backing file %s: %v", state.BackingFile, err)
	}
	if info.IsDir() {
		return nil, status.Errorf(codes.Internal, "backing file is a directory: %s", state.BackingFile)
	}

	loopDevice, err := d.ensureLoopDevice(state.BackingFile)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ensure loop device: %v", err)
	}

	if err := d.publishBlockAtTarget(loopDevice, req.GetTargetPath()); err != nil {
		return nil, status.Errorf(codes.Internal, "publish loop device: %v", err)
	}

	if state.PublishedTo == nil {
		state.PublishedTo = map[string]string{}
	}
	state.PublishedTo[req.GetTargetPath()] = loopDevice
	state.LoopDevice = loopDevice
	if deviceSymlink, symlinkErr := d.EnsureDeviceSymlink(state.VolumeID, loopDevice); symlinkErr == nil {
		state.DeviceSymlink = deviceSymlink
	}

	if err := d.saveVolumeStateByID(state); err != nil {
		return nil, status.Errorf(codes.Internal, "save volume state: %v", err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if d.cfg.Mode != "node" {
		return nil, status.Error(codes.Unimplemented, "node service not enabled")
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing volume_id")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target_path")
	}

	if d.isMounted(req.GetTargetPath()) {
		if err := unix.Unmount(req.GetTargetPath(), 0); err != nil {
			return nil, status.Errorf(codes.Internal, "unmount target %s: %v", req.GetTargetPath(), err)
		}
	}

	if err := os.Remove(req.GetTargetPath()); err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "remove target %s: %v", req.GetTargetPath(), err)
	}

	state, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil || state.PublishedTo == nil {
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	delete(state.PublishedTo, req.GetTargetPath())

	for targetPath := range state.PublishedTo {
		if _, statErr := os.Stat(targetPath); statErr != nil && os.IsNotExist(statErr) {
			delete(state.PublishedTo, targetPath)
		}
	}

	if len(state.PublishedTo) != 0 {
		_ = d.saveVolumeStateByID(state)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	detached, detachErr := d.detachLoopDeviceIfPresent(state.BackingFile)
	if detachErr != nil {
		return nil, status.Errorf(codes.Internal, "detach loop device: %v", detachErr)
	}
	if !detached {
		_ = d.saveVolumeStateByID(state)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	state.LoopDevice = ""
	state.DeviceSymlink = ""
	if removeErr := d.RemoveDeviceSymlink(state.VolumeID); removeErr != nil {
		return nil, status.Errorf(codes.Internal, "remove device symlink: %v", removeErr)
	}

	_ = d.saveVolumeStateByID(state)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (d *Driver) detachLoopDeviceIfPresent(backingFile string) (bool, error) {
	loopDevice, findErr := d.findLoopDeviceByBackingFile(backingFile)
	if findErr != nil || loopDevice == "" {
		return true, nil
	}

	loopNumber, parseErr := d.loopDeviceNumber(loopDevice)
	if parseErr != nil {
		return true, nil
	}

	if detachErr := d.losetupDeviceDetach(loopNumber); detachErr != nil {
		if errors.Is(detachErr, unix.EBUSY) {
			return false, nil
		}
		return false, fmt.Errorf("%s: %w", loopDevice, detachErr)
	}

	return true, nil
}

func (d *Driver) NodeExpandVolume(context.Context, *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (d *Driver) NodeGetVolumeStats(context.Context, *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (d *Driver) nodeCap(t csi.NodeServiceCapability_RPC_Type) *csi.NodeServiceCapability {
	return &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{Type: t},
		},
	}
}

func (d *Driver) isMounted(target string) bool {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	return strings.Contains(string(data), " "+target+" ")
}

func (d *Driver) publishBlockAtTarget(sourceDevice, targetPath string) error {
	if sourceDevice == "" {
		return fmt.Errorf("missing source device")
	}

	srcInfo, err := os.Stat(sourceDevice)
	if err != nil {
		return fmt.Errorf("stat source device %s: %w", sourceDevice, err)
	}
	srcStat, ok := srcInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unexpected stat type for %s", sourceDevice)
	}
	if (srcInfo.Mode() & os.ModeDevice) == 0 {
		return fmt.Errorf("source is not a device: %s", sourceDevice)
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("mkdir parent: %w", err)
	}

	if st, err := os.Lstat(targetPath); err == nil {
		if (st.Mode() & os.ModeDevice) != 0 {
			dstStat, ok := st.Sys().(*syscall.Stat_t)
			if !ok {
				return fmt.Errorf("unexpected stat type for %s", targetPath)
			}
			dstMajor := unix.Major(uint64(dstStat.Rdev))
			dstMinor := unix.Minor(uint64(dstStat.Rdev))
			srcMajor := unix.Major(uint64(srcStat.Rdev))
			srcMinor := unix.Minor(uint64(srcStat.Rdev))
			if dstMajor == srcMajor && dstMinor == srcMinor {
				return nil
			}
		}
		if !d.isMounted(targetPath) {
			if err := os.Remove(targetPath); err != nil {
				return fmt.Errorf("remove stale target %s: %w", targetPath, err)
			}
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("lstat target %s: %w", targetPath, err)
	}

	f, err := os.OpenFile(targetPath, os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("create target file %s: %w", targetPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close target file %s: %w", targetPath, err)
	}

	if d.isMounted(targetPath) {
		return nil
	}

	if err := unix.Mount(sourceDevice, targetPath, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind mount %s -> %s: %w", sourceDevice, targetPath, err)
	}

	return nil
}
