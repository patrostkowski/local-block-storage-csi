package plugin

import (
	"github.com/patrostkowski/local-block-storage-csi/internal"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

func NewCommand(baseCfg func() internal.Config) *cobra.Command {
	var nodeID string

	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "Run the CSI controller and node services for this node",
		Run: func(cmd *cobra.Command, args []string) {
			if nodeID == "" {
				klog.Fatalf("--node-id is required")
			}
			cfg := baseCfg()
			cfg.ControllerEnabled = true
			cfg.NodeEnabled = true
			cfg.NodeID = nodeID
			if err := internal.Run(cfg); err != nil {
				klog.Fatalf("plugin failed: %v", err)
			}
		},
	}

	cmd.Flags().StringVar(&nodeID, "node-id", "", "Kubernetes node name/ID for this instance")

	return cmd
}
