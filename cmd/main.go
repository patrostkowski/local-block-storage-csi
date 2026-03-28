package main

import (
	"flag"

	"github.com/patrostkowski/local-block-storage-csi/cmd/manager"
	"github.com/patrostkowski/local-block-storage-csi/cmd/node"
	"github.com/patrostkowski/local-block-storage-csi/internal"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

type commonConfig struct {
	endpoint  string
	dataRoot  string
	stateRoot string
	cleanup   bool
}

func main() {
	rootCfg := commonConfig{}

	klog.InitFlags(nil)

	rootCmd := &cobra.Command{
		Use:   "local-block-storage-csi",
		Short: "CSI driver for local loop-backed block volumes",
	}

	rootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	rootCmd.PersistentFlags().StringVar(&rootCfg.endpoint, "endpoint", internal.DefaultEndpoint, "CSI endpoint, e.g. unix:///csi/csi.sock")
	rootCmd.PersistentFlags().StringVar(&rootCfg.dataRoot, "data-root", internal.DefaultDataRoot, "Host path used for backing files")
	rootCmd.PersistentFlags().StringVar(&rootCfg.stateRoot, "state-root", internal.DefaultStateRoot, "Host path used for driver state")
	rootCmd.PersistentFlags().BoolVar(&rootCfg.cleanup, "cleanup-loop-devices", true, "Detach stale loop devices on startup and periodic node requests")
	baseCfg := func() internal.Config {
		return internal.Config{
			Endpoint:           rootCfg.endpoint,
			DataRoot:           rootCfg.dataRoot,
			StateRoot:          rootCfg.stateRoot,
			CleanupLoopDevices: rootCfg.cleanup,
			DeviceSymlinkRoot:  internal.DefaultDeviceSymlinkRoot,
		}
	}

	rootCmd.AddCommand(manager.NewCommand(baseCfg))
	rootCmd.AddCommand(node.NewCommand(baseCfg))

	if err := rootCmd.Execute(); err != nil {
		klog.Fatalf("command failed: %v", err)
	}
}
