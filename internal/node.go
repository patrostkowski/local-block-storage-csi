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

func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if d.cfg.Mode != "node" {
		return nil, status.Error(codes.Unimplemented, "node service not enabled")
	}
	klog.Infof("NodeStageVolume volumeID=%s stagingPath=%s", req.GetVolumeId(), req.GetStagingTargetPath())
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
	if state.NodeID != d.cfg.NodeID {
		return nil, status.Errorf(codes.FailedPrecondition, "volume belongs to node %q, this node is %q", state.NodeID, d.cfg.NodeID)
	}

	loopDevice, err := d.ensureLoopDevice(state.BackingFile)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ensure loop device: %v", err)
	}

	stageDir := req.GetStagingTargetPath()
	stageDevicePath := filepath.Join(stageDir, "block-device")

	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir staging dir: %v", err)
	}
	if err := d.publishBlockAtTarget(loopDevice, stageDevicePath); err != nil {
		return nil, status.Errorf(codes.Internal, "publish staged block device: %v", err)
	}

	state.LoopDevice = strings.TrimSpace(loopDevice)
	state.StagedPath = stageDir
	state.StagedDevicePath = stageDevicePath

	if err := d.saveVolumeStateByID(state); err != nil {
		return nil, status.Errorf(codes.Internal, "save volume state: %v", err)
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if d.cfg.Mode != "node" {
		return nil, status.Error(codes.Unimplemented, "node service not enabled")
	}
	klog.Infof("NodeUnstageVolume volumeID=%s", req.GetVolumeId())
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

	if state.StagedDevicePath != "" {
		_ = os.Remove(state.StagedDevicePath)
	}

	if state.PublishedTo != nil {
		for targetPath := range state.PublishedTo {
			if _, err := os.Stat(targetPath); err != nil {
				if os.IsNotExist(err) {
					delete(state.PublishedTo, targetPath)
				}
			}
		}
	}

	if len(state.PublishedTo) == 0 && state.LoopDevice != "" {
		loopNumber, err := d.loopDeviceNumber(state.LoopDevice)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "parse loop device %s: %v", state.LoopDevice, err)
		}
		if err := d.losetupDeviceDetach(loopNumber); err != nil {
			return nil, status.Errorf(codes.Internal, "detach loop device %s: %v", state.LoopDevice, err)
		}
		state.LoopDevice = ""
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
	klog.Infof("NodePublishVolume volumeID=%s targetPath=%s loopDevice=%s", req.GetVolumeId(), req.GetTargetPath(), state.LoopDevice)
	if state.NodeID != d.cfg.NodeID {
		return nil, status.Errorf(codes.FailedPrecondition, "volume belongs to node %q, this node is %q", state.NodeID, d.cfg.NodeID)
	}
	if state.LoopDevice == "" {
		return nil, status.Error(codes.FailedPrecondition, "volume is not staged")
	}

	sourceDevice := state.LoopDevice
	if state.StagedDevicePath != "" {
		sourceDevice = state.StagedDevicePath
	}

	if err := d.publishBlockAtTarget(sourceDevice, req.GetTargetPath()); err != nil {
		return nil, status.Errorf(codes.Internal, "publish block device: %v", err)
	}

	if state.PublishedTo == nil {
		state.PublishedTo = map[string]string{}
	}
	state.PublishedTo[req.GetTargetPath()] = sourceDevice

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

	_ = os.Remove(req.GetTargetPath())

	state, err := d.loadVolumeStateByID(req.GetVolumeId())
	if err == nil && state.PublishedTo != nil {
		delete(state.PublishedTo, req.GetTargetPath())
		_ = d.saveVolumeStateByID(state)
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

func (d *Driver) publishBlockAtTarget(loopDevice, targetPath string) error {
	if loopDevice == "" {
		return fmt.Errorf("missing loop device")
	}

	srcInfo, err := os.Stat(loopDevice)
	if err != nil {
		return fmt.Errorf("stat source device %s: %w", loopDevice, err)
	}
	srcStat, ok := srcInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unexpected stat type for %s", loopDevice)
	}
	if (srcInfo.Mode() & os.ModeDevice) == 0 {
		return fmt.Errorf("source is not a device: %s", loopDevice)
	}

	srcMajor := unix.Major(uint64(srcStat.Rdev))
	srcMinor := unix.Minor(uint64(srcStat.Rdev))
	dev := unix.Mkdev(srcMajor, srcMinor)

	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("mkdir parent: %w", err)
	}

	if st, err := os.Lstat(targetPath); err == nil {
		if (st.Mode() & os.ModeDevice) == 0 {
			if err := os.Remove(targetPath); err != nil {
				return fmt.Errorf("remove non-device target %s: %w", targetPath, err)
			}
		} else {
			dstStat, ok := st.Sys().(*syscall.Stat_t)
			if !ok {
				return fmt.Errorf("unexpected stat type for %s", targetPath)
			}
			dstMajor := unix.Major(uint64(dstStat.Rdev))
			dstMinor := unix.Minor(uint64(dstStat.Rdev))
			if dstMajor == srcMajor && dstMinor == srcMinor {
				return nil
			}
			if err := os.Remove(targetPath); err != nil {
				return fmt.Errorf("remove wrong device node %s: %w", targetPath, err)
			}
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("lstat target %s: %w", targetPath, err)
	}

	mode := uint32(unix.S_IFBLK | 0o600)
	if err := unix.Mknod(targetPath, mode, int(dev)); err != nil {
		return fmt.Errorf("mknod %s: %w", targetPath, err)
	}

	return nil
}
