package azcopy

import (
	"fmt"
	"net/url"
	"runtime"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

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
		(!source && (loc == common.ELocation.File() || loc == common.ELocation.FileNFS())) {
		// invalidChars := `<>\/:"|?*` + string(0x00)

		for k, c := range encodedInvalidCharacters {
			for part, p := range pathParts {
				pathParts[part] = strings.ReplaceAll(p, string(k), c)
			}
		}

		// If uploading from Windows or downloading from files, decode unsafe chars if user enables decoding
	} else if ((!source && fromTo.From() == common.ELocation.Local() && runtime.GOOS == "windows") ||
		(!source && (fromTo.From() == common.ELocation.File() || fromTo.From() == common.ELocation.FileNFS()))) && !disableAutoDecoding {

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
