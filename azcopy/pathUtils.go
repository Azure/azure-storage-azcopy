package azcopy

import (
	"fmt"
	"net/url"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func GetContainerName(path string, location common.Location) (string, error) {
	switch location {
	case common.ELocation.Local():
		panic("attempted to get container name on local location")
	case common.ELocation.Blob(),
		common.ELocation.File(),
		common.ELocation.FileNFS(),
		common.ELocation.BlobFS():
		bURLParts, err := sas.ParseURL(path)
		if err != nil {
			return "", err
		}
		return bURLParts.ContainerName, nil
	case common.ELocation.S3():
		baseURL, err := url.Parse(path)

		if err != nil {
			return "", err
		}

		s3URLParts, err := common.NewS3URLParts(*baseURL)

		if err != nil {
			return "", err
		}

		return s3URLParts.BucketName, nil
	case common.ELocation.GCP():
		baseURL, err := url.Parse(path)
		if err != nil {
			return "", err
		}
		gcpURLParts, err := common.NewGCPURLParts(*baseURL)
		if err != nil {
			return "", err
		}
		return gcpURLParts.BucketName, nil
	default:
		return "", fmt.Errorf("cannot get container name on location type %s", location.String())
	}
}
