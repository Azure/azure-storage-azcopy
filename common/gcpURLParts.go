package common

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

// GCPURLParts structure is used to parse and hold the different
// components of GCP Object/Service/Bucket URL
type GCPURLParts struct {
	Scheme         string
	Host           string
	BucketName     string
	ObjectKey      string
	UnparsedParams string
}

const gcpHostPattern = "^storage\\.cloud\\.google\\.com"
const invalidGCPURLErrorMessage = "Invalid GCP URL"
const gcpEssentialHostPart = "google.com"

var gcpHostRegex = regexp.MustCompile(gcpHostPattern)

// IsGCPURL validates whether a given URL is a valid GCP Object/Service/Bucket URL
func IsGCPURL(u url.URL) bool {
	if _, isGCPURL := findGCPURLMatches(strings.ToLower(u.Host)); isGCPURL {
		return true
	}
	return false
}

func findGCPURLMatches(lower string) ([]string, bool) {
	matches := gcpHostRegex.FindStringSubmatch(lower)
	if matches == nil || !strings.Contains(lower, gcpEssentialHostPart) {
		return nil, false
	}
	return matches, true
}

// NewGCPURLParts processes the given URL and returns a valid GCPURLParts
// structure that contains all the necessary components.
func NewGCPURLParts(u url.URL) (GCPURLParts, error) {
	host := strings.ToLower(u.Host)
	_, isGCPURL := findGCPURLMatches(host)
	if !isGCPURL {
		return GCPURLParts{}, errors.New(invalidGCPURLErrorMessage)
	}

	path := u.Path

	if path != "" && path[0] == '/' {
		path = path[1:]
	}

	up := GCPURLParts{
		Scheme: u.Scheme,
		Host:   host,
	}

	if bucketEndIndex := strings.Index(path, "/"); bucketEndIndex != -1 {
		up.BucketName = path[:bucketEndIndex]
		up.ObjectKey = path[bucketEndIndex+1:]
	} else {
		up.BucketName = path
	}
	up.UnparsedParams = u.RawQuery

	return up, nil
}

// URL returns a valid net/url.URL object initialised from the components of GCP URL
func (gUrl *GCPURLParts) URL() url.URL {
	path := ""

	if gUrl.BucketName != "" {
		path += "/" + gUrl.BucketName
		if gUrl.ObjectKey != "" {
			path += "/" + gUrl.ObjectKey
		}
	}

	rawQuery := gUrl.UnparsedParams

	u := url.URL{
		Scheme:   gUrl.Scheme,
		Host:     gUrl.Host,
		Path:     path,
		RawQuery: rawQuery,
	}
	return u
}

func (gUrl *GCPURLParts) String() string {
	u := gUrl.URL()
	return u.String()
}

func (gUrl *GCPURLParts) IsServiceSyntactically() bool {
	if gUrl.Host != "" && gUrl.BucketName == "" {
		return true
	}
	return false
}

func (gUrl *GCPURLParts) IsBucketSyntactically() bool {
	if gUrl.BucketName != "" && gUrl.ObjectKey == "" {
		return true
	}
	return false
}

func (gUrl *GCPURLParts) IsObjectSyntactically() bool {
	if gUrl.BucketName != "" && gUrl.ObjectKey != "" {
		return true
	}
	return false
}

// IsDirectorySyntactically returns true if the given GCPURLParts
// points to a directory or not based on the path.
func (gUrl *GCPURLParts) IsDirectorySyntactically() bool {
	if gUrl.IsObjectSyntactically() && strings.HasSuffix(gUrl.ObjectKey, "/") {
		return true
	}
	return false
}
