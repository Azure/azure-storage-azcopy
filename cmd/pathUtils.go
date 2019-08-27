package cmd

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/common"
)

func getPathBeforeFirstWildcard(path string) string {
	if strings.Index(path, "*") == -1 {
		return path
	}

	firstWCIndex := strings.Index(path, "*")
	result := replacePathSeparators(path[:firstWCIndex])
	lastSepIndex := strings.LastIndex(result, "/")
	result = result[:lastSepIndex+1]

	return result
}

type LocationLevel uint8

var ELocationLevel LocationLevel = 0

func (LocationLevel) Service() LocationLevel   { return 0 }
func (LocationLevel) Container() LocationLevel { return 1 }
func (LocationLevel) Object() LocationLevel    { return 2 } // An Object can be a directory or object.

// Uses syntax to assume the "level" of a location.
// This is typically used to
func determineLocationLevel(location string, locationType common.Location, source bool) (LocationLevel, error) {
	switch locationType {
	case common.ELocation.Local():
		level := LocationLevel(2)
		if strings.Contains(location, "*") {
			return 1, nil
		}

		if strings.HasSuffix(location, "/") {
			level = 1
		}

		if !source {
			return level, nil // Return the assumption.
		}

		fi, err := os.Stat(location)

		if err != nil {
			return level, nil // Return the assumption.
		}

		if fi.IsDir() {
			return 1, nil
		} else {
			return 2, nil
		}
	case common.ELocation.Blob(),
		common.ELocation.File(),
		common.ELocation.BlobFS():
		URL, err := url.Parse(location)

		if err != nil {
			return 0, err
		}

		// blobURLParts is the same format and doesn't care about endpoint
		bURL := azblob.NewBlobURLParts(*URL)

		if strings.Contains(bURL.ContainerName, "*") && bURL.BlobName != "" {
			return 0, errors.New("can't use a wildcarded container name and specific blob name in combination")
		}

		if bURL.BlobName != "" {
			return 2, nil
		} else if bURL.ContainerName != "" && !strings.Contains(bURL.ContainerName, "*") {
			return 1, nil
		} else {
			return 0, nil
		}
	case common.ELocation.S3():
		URL, err := url.Parse(location)

		if err != nil {
			return 0, nil
		}

		s3URL, err := common.NewS3URLParts(*URL)

		if err != nil {
			return 0, nil
		}

		if strings.Contains(s3URL.BucketName, "*") && s3URL.ObjectKey != "" {
			return 0, errors.New("can't use a wildcarded container name and specific object name in combination")
		}

		if s3URL.ObjectKey != "" {
			return 2, nil
		} else if s3URL.BucketName != "" && !strings.Contains(s3URL.BucketName, "*") {
			return 1, nil
		} else {
			return 0, nil
		}
	default: // Probably won't ever hit this
		return 0, fmt.Errorf("getting level of location is impossible on location %s", locationType)
	}
}

func GetAccountRoot(path string, location common.Location) (string, error) {
	switch location {
	case common.ELocation.Local():
		return path, nil // Probably won't be triggered, but just on the offchance
	case common.ELocation.Blob(),
		common.ELocation.File(),
		common.ELocation.BlobFS():
		baseURL, err := url.Parse(path)

		if err != nil {
			return "", err
		}

		// Clear the path
		bURLParts := azblob.NewBlobURLParts(*baseURL)
		bURLParts.ContainerName = ""
		bURLParts.BlobName = ""

		bURL := bURLParts.URL()
		return bURL.String(), nil
	default:
		return "", fmt.Errorf("cannot get account root on location type %s", location.String())
	}
}
