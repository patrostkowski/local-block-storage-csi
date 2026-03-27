package internal

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func (d *Driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          DriverName,
		VendorVersion: DriverVersion,
	}, nil
}

func (d *Driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	var caps []*csi.PluginCapability

	if d.cfg.Mode == "controller" {
		caps = append(caps, &csi.PluginCapability{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
				},
			},
		})
		caps = append(caps, &csi.PluginCapability{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
				},
			},
		})
	}

	caps = append(caps, &csi.PluginCapability{
		Type: &csi.PluginCapability_VolumeExpansion_{
			VolumeExpansion: &csi.PluginCapability_VolumeExpansion{
				Type: csi.PluginCapability_VolumeExpansion_OFFLINE,
			},
		},
	})

	return &csi.GetPluginCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *Driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}
