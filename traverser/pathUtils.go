package traverser

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// All of the below functions only really do one thing at the moment.
// They've been separated from copyEnumeratorInit.go in order to make the code more maintainable, should we want more destinations in the future.
func GetPathBeforeFirstWildcard(path string) string {
	if !strings.Contains(path, "*") {
		return path
	}

	firstWCIndex := strings.Index(path, "*")
	result := common.ConsolidatePathSeparators(path[:firstWCIndex])
	lastSepIndex := strings.LastIndex(result, common.DeterminePathSeparator(path))
	result = result[:lastSepIndex+1]

	return result
}

type CopyHandlerUtil struct{}

// checks if a given url points to a container or virtual directory, as opposed to a blob or prefix match
func (util CopyHandlerUtil) urlIsContainerOrVirtualDirectory(rawURL string) bool {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	blobURLParts, err := blob.ParseURL(rawURL)
	if err != nil {
		return false
	}
	if blobURLParts.IPEndpointStyleInfo.AccountName == "" {
		// Typical endpoint style
		// If there's no slashes after the first, it's a container.
		// If there's a slash on the end, it's a virtual directory/container.
		// Otherwise, it's just a blob.
		if len(parsedURL.Path) == 0 {
			return true // We know for SURE that it's a account level URL
		}

		return strings.HasSuffix(parsedURL.Path, "/") || strings.Count(parsedURL.Path[1:], "/") == 0
	} else {
		// IP endpoint style: https://IP:port/accountname/container
		// If there's 2 or less slashes after the first, it's a container.
		// OR If there's a slash on the end, it's a virtual directory/container.
		// Otherwise, it's just a blob.
		return strings.HasSuffix(parsedURL.Path, "/") || strings.Count(parsedURL.Path[1:], "/") <= 1
	}
}

// redactSigQueryParam checks for the signature in the given rawquery part of the url
// If the signature exists, it replaces the value of the signature with "REDACTED"
// This api is used when SAS is written to log file to avoid exposing the user given SAS
// TODO: remove this, redactSigQueryParam could be added in SDK
func (util CopyHandlerUtil) redactSigQueryParam(rawQuery string) (bool, string) {
	rawQuery = strings.ToLower(rawQuery) // lowercase the string so we can look for ?sig= and &sig=
	sigFound := strings.Contains(rawQuery, "?"+common.SigAzure+"=")
	if !sigFound {
		sigFound = strings.Contains(rawQuery, "&"+common.SigAzure+"=")
		if !sigFound {
			return sigFound, rawQuery // [?|&]sig= not found; return same rawQuery passed in (no memory allocation)
		}
	}
	// [?|&]sig= found, redact its value
	values, _ := url.ParseQuery(rawQuery)
	for name := range values {
		if strings.EqualFold(name, common.SigAzure) {
			values[name] = []string{"REDACTED"}
		}
	}
	return sigFound, values.Encode()
}

// ConstructCommandStringFromArgs creates the user given commandString from the os Arguments
// If any argument passed is an http Url and contains the signature, then the signature is redacted
func (util CopyHandlerUtil) ConstructCommandStringFromArgs() string {
	// Get the os Args and strip away the first argument since it will be the path of Azcopy executable
	args := os.Args[1:]
	if len(args) == 0 {
		return ""
	}
	s := strings.Builder{}
	for _, arg := range args {
		// If the argument starts with http, it is either the remote source or remote destination
		// If there exists a signature in the argument string it needs to be redacted
		if StartsWith(arg, "http") {
			// parse the url
			argUrl, err := url.Parse(arg)
			// If there is an error parsing the url, then throw the error
			if err != nil {
				panic(fmt.Errorf("error parsing the url %s. Failed with error %s", arg, err.Error()))
			}
			// Check for the signature query parameter
			_, rawQuery := util.redactSigQueryParam(argUrl.RawQuery)
			argUrl.RawQuery = rawQuery
			s.WriteString(argUrl.String())
		} else {
			s.WriteString(arg)
		}
		s.WriteString(" ")
	}
	return s.String()
}

// doesBlobRepresentAFolder verifies whether blob is valid or not.
// Used to handle special scenarios or conditions.
func (util CopyHandlerUtil) doesBlobRepresentAFolder(metadata map[string]*string) bool {
	// this condition is to handle the WASB V1 directory structure.
	// HDFS driver creates a blob for the empty directories (let’s call it ‘myfolder’)
	// and names all the blobs under ‘myfolder’ as such: ‘myfolder/myblob’
	// The empty directory has meta-data 'hdi_isfolder = true'
	// Note: GoLang sometimes sets metadata keys with the first letter capitalized
	v, ok := common.TryReadMetadata(metadata, common.POSIXFolderMeta)
	return ok && v != nil && strings.ToLower(*v) == "true"
}

func StartsWith(s string, t string) bool {
	return len(s) >= len(t) && strings.EqualFold(s[0:len(t)], t)
}
