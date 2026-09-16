package azcopy

import hostinfointernal "github.com/Azure/azure-storage-azcopy/v10/azcopy/internal/hostinfo"

func probeAzureVM() bool {
	return hostinfointernal.AzurePublicCloudFromAssetTagFile("/sys/class/dmi/id/chassis_asset_tag")
}
