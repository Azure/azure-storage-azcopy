package azcopy

import (
	"io"
	"os"
)

func probeAzureVM() bool {
	return azurePublicCloudFromAssetTagFile("/sys/class/dmi/id/chassis_asset_tag")
}

func azurePublicCloudFromAssetTagFile(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	value, err := io.ReadAll(io.LimitReader(file, maxChassisAssetTagBytes+1))
	return err == nil && isAzurePublicCloudAssetTag(string(value))
}
