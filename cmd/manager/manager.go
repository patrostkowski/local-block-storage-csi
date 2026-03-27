package manager

import (
	"github.com/patrostkowski/local-block-storage-csi/internal"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

func NewCommand(baseCfg func() internal.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "manager",
		Short: "Run controller (provisioner) service",
		Run: func(cmd *cobra.Command, args []string) {
			cfg := baseCfg()
			cfg.Mode = "controller"
			if err := internal.Run(cfg); err != nil {
				klog.Fatalf("manager failed: %v", err)
			}
		},
	}

	return cmd
}
