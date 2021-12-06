package cmd

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/pkg/errors"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// ----- LOCATION LEVEL HANDLING -----
type LocationLevel uint8

var ELocationLevel LocationLevel = 0

func (LocationLevel) Service() LocationLevel   { return 0 }
func (LocationLevel) Container() LocationLevel { return 1 }
func (LocationLevel) Object() LocationLevel    { return 2 } // An Object can be a directory or object.

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
		return cleanLocalPath(getPathBeforeFirstWildcard(resource)), nil

	//noinspection GoNilness
	case common.ELocation.Blob():
		bURLParts := azblob.NewBlobURLParts(*resourceURL)

		if bURLParts.ContainerName == "" || strings.Contains(bURLParts.ContainerName, "*") {
			if bURLParts.BlobName != "" {
				return resource, errors.New("cannot combine account-level traversal and specific blob names.")
			}

			bURLParts.ContainerName = ""
		}

		bURL := bURLParts.URL()
		return bURL.String(), nil

	//noinspection GoNilness
	case common.ELocation.File():
		bURLParts := azfile.NewFileURLParts(*resourceURL)

		if bURLParts.ShareName == "" || strings.Contains(bURLParts.ShareName, "*") {
			if bURLParts.DirectoryOrFilePath != "" {
				return resource, errors.New("cannot combine account-level traversal and specific file/folder names.")
			}

			bURLParts.ShareName = ""
		}

		bURL := bURLParts.URL()
		return bURL.String(), nil

	//noinspection GoNilness
	case common.ELocation.BlobFS():
		bURLParts := azfile.NewFileURLParts(*resourceURL)

		if bURLParts.ShareName == "" || strings.Contains(bURLParts.ShareName, "*") {
			if bURLParts.DirectoryOrFilePath != "" {
				return resource, errors.New("cannot combine account-level traversal and specific file/folder names.")
			}

			bURLParts.ShareName = ""
		}

		bURL := bURLParts.URL()
		return bURL.String(), nil

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

func SplitResourceString(raw string, loc common.Location) (common.ResourceString, error) {
	sasless, sas, err := splitAuthTokenFromResource(raw, loc)
	if err != nil {
		return common.ResourceString{}, nil
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
		return cleanLocalPath(common.ToExtendedPath(resource)), "", nil
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
		common.ELocation.Unknown(): // cover for unknown as we treat that as garbage
		// Local and S3 don't feature URL-embedded tokens
		return resource, "", nil

	// Use resource-specific APIs that all mostly do the same thing, just on the off-chance they end up doing something slightly different in the future.
	// TODO: make GetAccountRoot and GetContainerName use their own specific APIs as well. It's _unlikely_ at best that the URL format will change drastically.
	//       but just on the off-chance that it does, I'd prefer if AzCopy could adapt adequately as soon as the SDK catches the change
	//       We've already seen a similar thing happen with Blob SAS tokens and the introduction of User Delegation Keys.
	//       It's not a breaking change to the way SAS tokens work, but a pretty major addition.
	// TODO: Find a clever way to reduce code duplication in here. Especially the URL parsing.
	case common.ELocation.Blob():
		var baseURL *url.URL // Do not shadow err for clean return statement
		baseURL, err = url.Parse(resource)

		if err != nil {
			return resource, "", err
		}

		bURLParts := azblob.NewBlobURLParts(*baseURL)
		resourceToken = bURLParts.SAS.Encode()
		bURLParts.SAS = azblob.SASQueryParameters{} // clear the SAS token and drop the raw, base URL
		blobURL := bURLParts.URL()                  // Can't call .String() on .URL() because Go can't take the pointer of a function's return
		resourceBase = blobURL.String()
		return
	case common.ELocation.File():
		var baseURL *url.URL // Do not shadow err for clean return statement
		baseURL, err = url.Parse(resource)

		if err != nil {
			return resource, "", err
		}

		fURLParts := azfile.NewFileURLParts(*baseURL)
		resourceToken = fURLParts.SAS.Encode()
		if resourceToken == "" {
			// Azure Files only supports the use of SAS tokens currently
			// Azure Files ALSO can't be a public resource
			// Therefore, it is safe to error here if no SAS token is present, as neither a source nor a destination could safely not have a SAS token.
			return resource, "", errors.New("azure files only supports the use of SAS token authentication")
		}
		fURLParts.SAS = azfile.SASQueryParameters{} // clear the SAS token and drop the raw, base URL
		fileURL := fURLParts.URL()                  // Can't call .String() on .URL() because Go can't take the pointer of a function's return
		resourceBase = fileURL.String()
		return
	case common.ELocation.BlobFS():
		var baseURL *url.URL // Do not shadow err for clean return statement
		baseURL, err = url.Parse(resource)

		if err != nil {
			return resource, "", err
		}

		bfsURLParts := azbfs.NewBfsURLParts(*baseURL)
		resourceToken = bfsURLParts.SAS.Encode()
		bfsURLParts.SAS = azbfs.SASQueryParameters{}
		bfsURL := bfsURLParts.URL() // Can't call .String() on .URL() because Go can't take the pointer of a function's return
		resourceBase = bfsURL.String()
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
		// we wil persist that (but we don't want to persist SAS's)
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
func getPathBeforeFirstWildcard(path string) string {
	if strings.Index(path, "*") == -1 {
		return path
	}

	firstWCIndex := strings.Index(path, "*")
	result := common.ConsolidatePathSeparators(path[:firstWCIndex])
	lastSepIndex := strings.LastIndex(result, common.DeterminePathSeparator(path))
	result = result[:lastSepIndex+1]

	return result
}

func GetAccountRoot(resource common.ResourceString, location common.Location) (string, error) {
	switch location {
	case common.ELocation.Local():
		panic("attempted to get account root on local location")
	case common.ELocation.Blob(),
		common.ELocation.File(),
		common.ELocation.BlobFS():
		baseURL, err := resource.FullURL()

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

func GetContainerName(path string, location common.Location) (string, error) {
	switch location {
	case common.ELocation.Local():
		panic("attempted to get container name on local location")
	case common.ELocation.Blob(),
		common.ELocation.File(),
		common.ELocation.BlobFS():
		baseURL, err := url.Parse(path)

		if err != nil {
			return "", err
		}

		bURLParts := azblob.NewBlobURLParts(*baseURL)
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
