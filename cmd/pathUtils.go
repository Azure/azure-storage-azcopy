package cmd

import (
	"fmt"
	"net/url"
	"strings"

	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	datalakesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/sas"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/pkg/errors"
)

// ----- ROOT PATH GRABBING -----

// GetResourceRoot should eliminate wildcards and error out in invalid scenarios. This is intended for the jobPartOrder.SourceRoot.
func GetResourceRoot(resource string, location common.Location) (resourceBase string, err error) {
	// Don't error-check this until we are in a supported environment
	resourceURL, err := url.Parse(resource)

	if location.IsRemote() && err != nil {
		return resource, err
	}

	// todo: reduce code-delicateness, maybe?
	switch location {
	case common.ELocation.Unknown(),
		common.ELocation.Benchmark(): // do nothing
		return resource, nil
	case common.ELocation.Local():
		return common.CleanLocalPath(traverser.GetPathBeforeFirstWildcard(resource)), nil

	//noinspection GoNilness
	case common.ELocation.Blob():
		bURLParts, err := blobsas.ParseURL(resource)
		if err != nil {
			return resource, err
		}

		if bURLParts.ContainerName == "" || strings.Contains(bURLParts.ContainerName, "*") {
			if bURLParts.BlobName != "" {
				return resource, errors.New("cannot combine account-level traversal and specific blob names.")
			}

			bURLParts.ContainerName = ""
		}

		return bURLParts.String(), nil

	//noinspection GoNilness
	case common.ELocation.File(), common.ELocation.FileNFS():
		fURLParts, err := filesas.ParseURL(resource)
		if err != nil {
			return resource, err
		}

		if fURLParts.ShareName == "" || strings.Contains(fURLParts.ShareName, "*") {
			if fURLParts.DirectoryOrFilePath != "" {
				return resource, errors.New("cannot combine account-level traversal and specific file/folder names.")
			}

			fURLParts.ShareName = ""
		}

		return fURLParts.String(), nil

	//noinspection GoNilness
	case common.ELocation.BlobFS():
		dURLParts, err := datalakesas.ParseURL(resource)
		if err != nil {
			return resource, err
		}

		if dURLParts.FileSystemName == "" || strings.Contains(dURLParts.FileSystemName, "*") {
			if dURLParts.PathName != "" {
				return resource, errors.New("cannot combine account-level traversal and specific file/folder names.")
			}

			dURLParts.FileSystemName = ""
		}

		return dURLParts.String(), nil

	// noinspection GoNilness
	case common.ELocation.S3():
		s3URLParts, err := common.NewS3URLParts(*resourceURL)
		common.PanicIfErr(err)

		if s3URLParts.BucketName == "" || strings.Contains(s3URLParts.BucketName, "*") {
			if s3URLParts.ObjectKey != "" {
				return resource, errors.New("cannot combine account-level traversal and specific object names")
			}

			s3URLParts.BucketName = ""
		}

		s3URL := s3URLParts.URL()
		return s3URL.String(), nil
	case common.ELocation.GCP():
		gcpURLParts, err := common.NewGCPURLParts(*resourceURL)
		common.PanicIfErr(err)

		if gcpURLParts.BucketName == "" || strings.Contains(gcpURLParts.BucketName, "*") {
			if gcpURLParts.ObjectKey != "" {
				return resource, errors.New("Cannot combine account-level traversal and specific object names")
			}
			gcpURLParts.BucketName = ""
		}

		gcpURL := gcpURLParts.URL()
		return gcpURL.String(), nil
	default:
		panic(fmt.Sprintf("Location %s is missing from GetResourceRoot", location))
	}
}
