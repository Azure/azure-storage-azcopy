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

// ----- LOCATION LEVEL HANDLING -----
type LocationLevel uint8

var ELocationLevel LocationLevel = 0

func (LocationLevel) Account() LocationLevel   { return 0 } // Account is never used in AzCopy, but is in testing to understand resource management.
func (LocationLevel) Service() LocationLevel   { return 1 }
func (LocationLevel) Container() LocationLevel { return 2 }
func (LocationLevel) Object() LocationLevel    { return 3 } // An Object can be a directory or object.

// Uses syntax to assume the "level" of a location.
// This is typically used to
func DetermineLocationLevel(location string, locationType common.Location, source bool) (LocationLevel, error) {
	switch locationType {
	// In local, there's no such thing as a service.
	// As such, we'll treat folders as containers, and files as objects.
	case common.ELocation.Local():
		level := LocationLevel(ELocationLevel.Object())
		if strings.Contains(location, "*") {
			return ELocationLevel.Container(), nil
		}

		if strings.HasSuffix(location, "/") {
			level = ELocationLevel.Container()
		}

		if !source {
			return level, nil // Return the assumption.
		}

		fi, err := common.OSStat(location)

		if err != nil {
			return level, nil // Return the assumption.
		}

		if fi.IsDir() {
			return ELocationLevel.Container(), nil
		} else {
			return ELocationLevel.Object(), nil
		}
	case common.ELocation.Benchmark():
		return ELocationLevel.Object(), nil // we always benchmark to a subfolder, not the container root

	case common.ELocation.Blob(),
		common.ELocation.File(),
		common.ELocation.FileNFS(),
		common.ELocation.BlobFS(),
		common.ELocation.S3(),
		common.ELocation.GCP():
		URL, err := url.Parse(location)

		if err != nil {
			return ELocationLevel.Service(), err
		}

		// GenericURLParts determines the correct resource URL parts to make use of
		bURL := common.NewGenericResourceURLParts(*URL, locationType)

		if strings.Contains(bURL.GetContainerName(), "*") && bURL.GetObjectName() != "" {
			return ELocationLevel.Service(), errors.New("can't use a wildcarded container name and specific blob name in combination")
		}

		if bURL.GetObjectName() != "" {
			return ELocationLevel.Object(), nil
		} else if bURL.GetContainerName() != "" && !strings.Contains(bURL.GetContainerName(), "*") {
			return ELocationLevel.Container(), nil
		} else {
			return ELocationLevel.Service(), nil
		}
	default: // Probably won't ever hit this
		return ELocationLevel.Service(), fmt.Errorf("getting level of location is impossible on location %s", locationType)
	}
}

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

func GetContainerName(path string, location common.Location) (string, error) {
	switch location {
	case common.ELocation.Local():
		panic("attempted to get container name on local location")
	case common.ELocation.Blob(),
		common.ELocation.File(),
		common.ELocation.FileNFS(),
		common.ELocation.BlobFS():
		bURLParts, err := blobsas.ParseURL(path)
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
