package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
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

func (d *Driver) loadAndValidateVolume(volumeID string) (*Volume, error) {
	state, err := d.loadVolumeStateByID(volumeID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "load volume state: %v", err)
	}
	if state.NodeID != d.cfg.NodeID {
		return nil, status.Errorf(codes.FailedPrecondition, "volume belongs to node %q, this node is %q", state.NodeID, d.cfg.NodeID)
	}
	if state.BackingFile == "" {
		return nil, status.Error(codes.FailedPrecondition, "backing file is empty")
	}
	info, err := os.Stat(state.BackingFile.Path())
	if err != nil {
		return nil, status.Error(StatusCode(err), err.Error())
	}
	if info.IsDir() {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("backing file is a directory: %s", state.BackingFile.Path()))
	}
	return state, nil
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

	vol, err := d.loadAndValidateVolume(req.GetVolumeId())
	if err != nil {
		return nil, err
	}

	if err := vol.EnsureLoopDevice(); err != nil {
		return nil, status.Error(StatusCode(err), err.Error())
	}
	vol.Symlink()

	if err := vol.Save(); err != nil {
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

	vol, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil {
		if os.IsNotExist(err) {
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "load volume state: %v", err)
	}

	if vol.PublishedTo != nil {
		for targetPath := range vol.PublishedTo {
			if _, statErr := os.Stat(targetPath); statErr != nil && os.IsNotExist(statErr) {
				delete(vol.PublishedTo, targetPath)
			}
		}
	}

	if detachErr := vol.DetachLoopDevice(); detachErr != nil {
		klog.Warningf("NodeUnstageVolume failed to detach loop device volumeID=%s: %v", vol.VolumeID, detachErr)
	}
	if removeErr := vol.RemoveSymlink(); removeErr != nil {
		klog.Warningf("NodeUnstageVolume failed removing device symlink volumeID=%s: %v", vol.VolumeID, removeErr)
	}

	if vol.BackingFile != "" {
		if _, statErr := os.Stat(vol.BackingFile.Path()); os.IsNotExist(statErr) {
			vol.Delete()
			klog.Infof("NodeUnstageVolume cleaned up state for deleted volume volumeID=%s", vol.VolumeID)
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
	}

	if err := vol.Save(); err != nil {
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

	vol, err := d.loadAndValidateVolume(req.GetVolumeId())
	if err != nil {
		return nil, err
	}

	if err := vol.EnsureLoopDevice(); err != nil {
		return nil, status.Error(StatusCode(err), err.Error())
	}

	if err := vol.Publish(req.GetTargetPath()); err != nil {
		return nil, status.Error(StatusCode(err), err.Error())
	}

	vol.Symlink()

	if err := vol.Save(); err != nil {
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

	vol, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err != nil || vol.PublishedTo == nil {
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	if unpublishErr := vol.Unpublish(req.GetTargetPath()); unpublishErr != nil {
		return nil, status.Error(StatusCode(unpublishErr), unpublishErr.Error())
	}

	if saveErr := vol.Save(); saveErr != nil {
		return nil, status.Errorf(codes.Internal, "save volume state: %v", saveErr)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
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
