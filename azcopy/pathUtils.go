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
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strings"

	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	datalakesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/sas"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

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
		level := ELocationLevel.Object()
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
	case common.ELocation.Benchmark(),
		common.ELocation.Pipe():
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

// Because some invalid characters weren't being properly encoded by url.PathEscape, we're going to instead manually encode them.
var encodedInvalidCharacters = map[rune]string{
	'<':  "%3C",
	'>':  "%3E",
	'\\': "%5C",
	'/':  "%2F",
	':':  "%3A",
	'"':  "%22",
	'|':  "%7C",
	'?':  "%3F",
	'*':  "%2A",
}

var reverseEncodedChars = map[string]rune{
	"%3C": '<',
	"%3E": '>',
	"%5C": '\\',
	"%2F": '/',
	"%3A": ':',
	"%22": '"',
	"%7C": '|',
	"%3F": '?',
	"%2A": '*',
}

func PathEncodeRules(path string, fromTo common.FromTo, disableAutoDecoding bool, source bool) string {
	var loc common.Location

	if source {
		loc = fromTo.From()
	} else {
		loc = fromTo.To()
	}
	pathParts := strings.Split(path, common.AZCOPY_PATH_SEPARATOR_STRING)

	// If downloading on Windows or uploading to files, encode unsafe characters.
	if (loc == common.ELocation.Local() && !source && runtime.GOOS == "windows") ||
		(!source && loc == common.ELocation.File()) {
		// invalidChars := `<>\/:"|?*` + string(0x00)

		for k, c := range encodedInvalidCharacters {
			for part, p := range pathParts {
				pathParts[part] = strings.ReplaceAll(p, string(k), c)
			}
		}

		// If uploading from Windows or downloading from files, decode unsafe chars if user enables decoding

		// Encoding is intended to handle behavior going between two resources.
		// For deletions, There isn't an actual "other resource"
		// So, the path special char does not to be decoded but preserved.
		// Why? Take an edge case where path contains special char like '%5C' (encoded backslash `\\`)
		// this will be decoded and error to inconsistent path separators.
	} else if ((!source && fromTo.From() == common.ELocation.Local() && runtime.GOOS == "windows") ||
		(!source && fromTo.From() == common.ELocation.File())) && !disableAutoDecoding && !fromTo.IsDelete() {

		for encoded, c := range reverseEncodedChars {
			for k, p := range pathParts {
				pathParts[k] = strings.ReplaceAll(p, encoded, string(c))
			}
		}
	}

	if loc.IsRemote() {
		for k, p := range pathParts {
			pathParts[k] = url.PathEscape(p)
		}
	}

	path = strings.Join(pathParts, "/")
	return path
}

// returns result of stripping and if striptopdir is enabled
// if nothing happens, the original source is returned
func StripTrailingWildcardOnRemoteSource(source string, location common.Location) (result string, stripTopDir bool, err error) {
	result = source
	resourceURL, err := url.Parse(result)
	gURLParts := common.NewGenericResourceURLParts(*resourceURL, location)

	if err != nil {
		err = fmt.Errorf("failed to parse url %s; %w", result, err)
		return
	}

	if strings.Contains(gURLParts.GetContainerName(), "*") {
		// Disallow container name search and object specifics
		if gURLParts.GetObjectName() != "" {
			err = errors.New("cannot combine a specific object name with an account-level search")
			return
		}

		// Return immediately here because we know this will be safe.
		return
	}

	// Trim the trailing /*.
	if strings.HasSuffix(resourceURL.RawPath, "/*") {
		resourceURL.RawPath = strings.TrimSuffix(resourceURL.RawPath, "/*")
		resourceURL.Path = strings.TrimSuffix(resourceURL.Path, "/*")
		stripTopDir = true
	}

	// Ensure there aren't any extra *s floating around.
	if strings.Contains(resourceURL.RawPath, "*") {
		err = errors.New("cannot use wildcards in the path section of the URL except in trailing \"/*\". If you wish to use * in your URL, manually encode it to %2A")
		return
	}

	result = resourceURL.String()

	return
}

// NormalizeResourceRoot should eliminate wildcards and error out in invalid scenarios. This is intended for the jobPartOrder.SourceRoot.
func NormalizeResourceRoot(resource string, location common.Location) (resourceBase string, err error) {
	// Don't error-check this until we are in a supported environment
	resourceURL, err := url.Parse(resource)

	if location.IsRemote() && err != nil {
		return resource, err
	}

	// todo: reduce code-delicateness, maybe?
	switch location {
	case common.ELocation.Unknown(),
		common.ELocation.Pipe(),
		common.ELocation.Benchmark(): // do nothing
		return resource, nil
	case common.ELocation.Local():
		return traverser.CleanLocalPath(traverser.GetPathBeforeFirstWildcard(resource)), nil

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
				return resource, errors.New("cannot combine account-level traversal and specific object names")
			}
			gcpURLParts.BucketName = ""
		}

		gcpURL := gcpURLParts.URL()
		return gcpURL.String(), nil
	default:
		panic(fmt.Sprintf("Location %s is missing from NormalizeResourceRoot", location))
	}
}
