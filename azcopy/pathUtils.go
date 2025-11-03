// Copyright © 2025 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package azcopy

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/pkg/errors"
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

func StartsWith(s string, t string) bool {
	return len(s) >= len(t) && strings.EqualFold(s[0:len(t)], t)
}
