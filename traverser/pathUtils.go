package traverser

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	datalakesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/sas"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func SplitResourceString(raw string, loc common.Location) (common.ResourceString, error) {
	sasless, sas, err := splitAuthTokenFromResource(raw, loc)
	if err != nil {
		return common.ResourceString{}, err
	}
	main, query := splitQueryFromSaslessResource(sasless, loc)
	return common.ResourceString{
		Value:      main,
		SAS:        sas,
		ExtraQuery: query,
	}, nil
}

// resourceBase will always be returned regardless of the location.
// resourceToken will be separated and returned depending on the location.
func splitAuthTokenFromResource(resource string, location common.Location) (resourceBase, resourceToken string, err error) {
	switch location {
	case common.ELocation.Local():
		if resource == common.Dev_Null {
			return resource, "", nil // don't mess with the special dev-null path, at all
		}
		return common.CleanLocalPath(common.ToExtendedPath(resource)), "", nil
	case common.ELocation.Pipe():
		return resource, "", nil
	case common.ELocation.S3():
		// Encoding +s as %20 (space) is important in S3 URLs as this is unsupported in Azure (but %20 can still be used as a space in S3 URLs)
		var baseURL *url.URL
		baseURL, err = url.Parse(resource)

		if err != nil {
			return resource, "", err
		}

		*baseURL = common.URLExtension{URL: *baseURL}.URLWithPlusDecodedInPath()
		return baseURL.String(), "", nil
	case common.ELocation.GCP():
		return resource, "", nil
	case common.ELocation.Benchmark(), // cover for benchmark as we generate data for that
		common.ELocation.Unknown(), // cover for unknown as we treat that as garbage
		common.ELocation.None():
		// Local and S3 don't feature URL-embedded tokens
		return resource, "", nil

	// Use resource-specific APIs that all mostly do the same thing, just on the off-chance they end up doing something slightly different in the future.
	// TODO: make GetAccountRoot and GetContainerName use their own specific APIs as well. It's _unlikely_ at best that the URL format will change drastically.
	//       but just on the off-chance that it does, I'd prefer if AzCopy could adapt adequately as soon as the SDK catches the change
	//       We've already seen a similar thing happen with Blob SAS tokens and the introduction of User Delegation Keys.
	//       It's not a breaking change to the way SAS tokens work, but a pretty major addition.
	// TODO: Find a clever way to reduce code duplication in here. Especially the URL parsing.
	case common.ELocation.Blob():
		var bURLParts blobsas.URLParts
		bURLParts, err = blobsas.ParseURL(resource)
		if err != nil {
			return resource, "", err
		}

		resourceToken = bURLParts.SAS.Encode()
		bURLParts.SAS = blobsas.QueryParameters{} // clear the SAS token and drop the raw, base URL
		resourceBase = bURLParts.String()
		return
	case common.ELocation.File(), common.ELocation.FileNFS():
		var fURLParts filesas.URLParts
		fURLParts, err = filesas.ParseURL(resource)
		if err != nil {
			return resource, "", err
		}

		resourceToken = fURLParts.SAS.Encode()
		fURLParts.SAS = filesas.QueryParameters{} // clear the SAS token and drop the raw, base URL
		resourceBase = fURLParts.String()
		return
	case common.ELocation.BlobFS():
		var dURLParts datalakesas.URLParts
		dURLParts, err = datalakesas.ParseURL(resource)
		if err != nil {
			return resource, "", err
		}

		resourceToken = dURLParts.SAS.Encode()
		dURLParts.SAS = datalakesas.QueryParameters{} // clear the SAS token and drop the raw, base URL
		resourceBase = dURLParts.String()
		return
	default:
		panic(fmt.Sprintf("One or more location(s) may be missing from SplitAuthTokenFromResource. Location: %s", location))
	}
}

// While there should be on SAS's in resource, it may have other query string elements,
// such as a snapshot identifier, or other unparsed params. This splits those out,
// so we can preserve them without having them get in the way of our use of
// the resource root string. (e.g. don't want them right on the end of it, when we append stuff)
func splitQueryFromSaslessResource(resource string, loc common.Location) (mainUrl string, queryAndFragment string) {
	if !loc.IsRemote() {
		return resource, "" // only remote resources have query strings
	}

	if u, err := url.Parse(resource); err == nil && u.Query().Get("sig") != "" {
		panic("this routine can only be called after the SAS has been removed")
		// because, for security reasons, we don't want SASs returned in queryAndFragment, since
		// we will persist that (but we don't want to persist SAS's)
	}

	// Work directly with a string-based format, so that we get both snapshot identifiers AND any other unparsed params
	// (types like BlobUrlParts handle those two things in separate properties, but return them together in the query string)
	i := strings.Index(resource, "?") // only the first ? is syntactically significant in a URL
	if i < 0 {
		return resource, ""
	} else if i == len(resource)-1 {
		return resource[:i], ""
	} else {
		return resource[:i], resource[i+1:]
	}
}

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
