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

	if state.LoopDevice != "" && state.StagedPath == req.GetStagingTargetPath() {
		klog.Infof("NodeStageVolume idempotent volumeID=%s loopDevice=%s", state.VolumeID, state.LoopDevice)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	loopDevice, err := d.ensureLoopDevice(state.BackingFile)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ensure loop device: %v", err)
	}
	klog.Infof("NodeStageVolume attached volumeID=%s loopDevice=%s", state.VolumeID, loopDevice)

	state.LoopDevice = strings.TrimSpace(loopDevice)
	state.StagedPath = req.GetStagingTargetPath()

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

	if state.LoopDevice != "" {
		klog.Infof("NodeUnstageVolume detach volumeID=%s loopDevice=%s", state.VolumeID, state.LoopDevice)
		if loopNumber, err := d.loopDeviceNumber(state.LoopDevice); err == nil {
			_ = d.losetupDeviceDetach(loopNumber)
		}
		state.LoopDevice = ""
	}
	state.StagedPath = ""

	if err := d.saveVolumeStateByID(state); err != nil {
		return nil, status.Errorf(codes.Internal, "save volume state: %v", err)
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if d.cfg.Mode != "node" {
		return nil, status.Error(codes.Unimplemented, "node service not enabled")
	}
	klog.Infof("NodePublishVolume volumeID=%s targetPath=%s", req.GetVolumeId(), req.GetTargetPath())
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
	if state.LoopDevice == "" {
		return nil, status.Error(codes.FailedPrecondition, "volume is not staged")
	}

	target := req.GetTargetPath()
	if err := d.ensureBlockTargetDevice(target, state.LoopDevice); err != nil {
		return nil, status.Errorf(codes.Internal, "prepare target device: %v", err)
	}
	if altTarget := d.alternateBlockDevicePath(target); altTarget != "" {
		if err := d.bindMountLoopDevice(state.LoopDevice, altTarget); err != nil {
			return nil, status.Errorf(codes.Internal, "bind-mount alternate target device: %v", err)
		}
		klog.Infof("NodePublishVolume bind-mounted alternate volumeID=%s loopDevice=%s targetPath=%s", state.VolumeID, state.LoopDevice, altTarget)
	}
	klog.Infof("NodePublishVolume device node ready volumeID=%s loopDevice=%s targetPath=%s", state.VolumeID, state.LoopDevice, target)

	if state.PublishedTo == nil {
		state.PublishedTo = map[string]string{}
	}
	state.PublishedTo[target] = target

	if err := d.saveVolumeStateByID(state); err != nil {
		return nil, status.Errorf(codes.Internal, "save volume state: %v", err)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if d.cfg.Mode != "node" {
		return nil, status.Error(codes.Unimplemented, "node service not enabled")
	}
	klog.Infof("NodeUnpublishVolume volumeID=%s targetPath=%s", req.GetVolumeId(), req.GetTargetPath())
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing volume_id")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing target_path")
	}

	if d.isMounted(req.GetTargetPath()) {
		if err := syscall.Unmount(req.GetTargetPath(), 0); err != nil {
			return nil, status.Errorf(codes.Internal, "unmount %s: %v", req.GetTargetPath(), err)
		}
	}
	_ = os.Remove(req.GetTargetPath())
	if altTarget := d.alternateBlockDevicePath(req.GetTargetPath()); altTarget != "" {
		if d.isMounted(altTarget) {
			_ = syscall.Unmount(altTarget, 0)
		}
		_ = os.Remove(altTarget)
	}

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

func (d *Driver) bindMountLoopDevice(loopDevice, targetPath string) error {
	if loopDevice == "" {
		return fmt.Errorf("missing loop device")
	}
	if err := d.ensureBindMountTarget(targetPath); err != nil {
		return err
	}
	if d.isMounted(targetPath) {
		return nil
	}
	return unix.Mount(loopDevice, targetPath, "", unix.MS_BIND, "")
}

func (d *Driver) ensureBlockTargetDevice(targetPath, loopDevice string) error {
	if loopDevice == "" {
		return fmt.Errorf("missing loop device")
	}
	info, err := os.Stat(loopDevice)
	if err != nil {
		return fmt.Errorf("stat loop device: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unexpected stat type for %s", loopDevice)
	}
	major := unix.Major(uint64(stat.Rdev))
	minor := unix.Minor(uint64(stat.Rdev))
	dev := unix.Mkdev(major, minor)

	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return err
	}

	if st, err := os.Stat(targetPath); err == nil {
		if (st.Mode() & os.ModeDevice) == 0 {
			klog.Warningf("target exists and is not a device node, replacing: %s", targetPath)
			if err := os.Remove(targetPath); err != nil {
				return fmt.Errorf("remove non-device target: %w", err)
			}
		} else {
			return nil
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	mode := uint32(unix.S_IFBLK | 0o600)
	return unix.Mknod(targetPath, mode, int(dev))
}

func (d *Driver) isMounted(target string) bool {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	return strings.Contains(string(data), " "+target+" ")
}

func (d *Driver) ensureBindMountTarget(targetPath string) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(targetPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	file, err := os.OpenFile(targetPath, os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	return file.Close()
}

func (d *Driver) alternateBlockDevicePath(targetPath string) string {
	const kubeletPrefix = "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/"
	if !strings.HasPrefix(targetPath, kubeletPrefix+"publish/") {
		return ""
	}
	trimmed := strings.TrimPrefix(targetPath, kubeletPrefix+"publish/")
	parts := strings.Split(trimmed, string(os.PathSeparator))
	if len(parts) < 2 {
		return ""
	}
	specName := parts[0]
	podUID := parts[1]
	if specName == "" || podUID == "" {
		return ""
	}
	return filepath.Join(kubeletPrefix, specName, "dev", podUID)
}
