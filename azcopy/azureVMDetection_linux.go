//go:build linux

package azcopy

import hostinfointernal "github.com/Azure/azure-storage-azcopy/v10/azcopy/internal/hostinfo"

// See https://learn.microsoft.com/en-us/azure/virtual-machines/identify-azure-vm-from-guest
func probeAzureVM() bool {
	return hostinfointernal.AzurePublicCloudFromAssetTagFile("/sys/class/dmi/id/chassis_asset_tag")
}
