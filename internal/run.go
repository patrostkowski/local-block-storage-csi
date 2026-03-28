package internal

import (
	"net"
	"os"
	"path/filepath"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

func Run(cfg Config) error {
	klog.Infof("starting driver mode=%s endpoint=%s dataRoot=%s stateRoot=%s", cfg.Mode, cfg.Endpoint, cfg.DataRoot, cfg.StateRoot)
	if err := os.MkdirAll(filepath.Join(cfg.DataRoot, "volumes"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(cfg.StateRoot, "volumes"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(cfg.StateRoot, "publish"), 0o755); err != nil {
		return err
	}
	address, cleanup, err := ParseEndpoint(cfg.Endpoint)
	if err != nil {
		return err
	}
	defer cleanup()

	listener, err := net.Listen("unix", address)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(address) }()

	server := grpc.NewServer()
	driverInstance := New(cfg)
	if cfg.Mode == "node" {
		if cfg.CleanupLoopDevices {
			if err := driverInstance.CleanupLoopDevices(); err != nil {
				klog.Warningf("loop device cleanup failed: %v", err)
			}
		}
		driverInstance.reconcileVolumeSymlinks()
	}
	csi.RegisterIdentityServer(server, driverInstance)
	if cfg.Mode == "controller" {
		csi.RegisterControllerServer(server, driverInstance)
	}
	if cfg.Mode == "node" {
		csi.RegisterNodeServer(server, driverInstance)
	}

	if cfg.Mode == "node" {
		klog.Infof("%s %s listening on %s (mode=%s nodeID=%s)",
			DriverName, DriverVersion, cfg.Endpoint, cfg.Mode, cfg.NodeID)
	} else {
		klog.Infof("%s %s listening on %s (mode=%s)",
			DriverName, DriverVersion, cfg.Endpoint, cfg.Mode)
	}

	return server.Serve(listener)
}
