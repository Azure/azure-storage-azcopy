// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package common

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

// S3URLParts represents the components that make up AWS, Wasabi, BackBlaze, DigitalOcean or MinIO S3 Service/Bucket/Object URL.
// You parse an existing URL into its parts by calling NewS3URLParts().
// According to http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro and
// https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region,
//
// For AWS, Wasabi, BackBlaze:
// S3URLParts supports virtual-hosted-style and path-style URL:
// Ex, virtual-hosted-style(the bucket name is part of the domain name in the URL) :
// a. http://bucket.s3.amazonaws.com
// b. http://bucket.s3-aws-region.amazonaws.com
// Ex,  path-style URL(the bucket name is not part of the domain (unless you use a Region-specific endpoint)):
// a. http://s3.amazonaws.com/bucket (US East (N. Virginia) Region endpoint)
// b. http://s3-aws-region.amazonaws.com/bucket (Region-specific endpoint)
// For DigitalOcean Spaces:
// - Virtual-style: https://bucket.nyc3.digitaloceanspaces.com
// - Path-style: https://nyc3.digitaloceanspaces.com/bucket
//
// For MinIO:
// - Virtual-style: https://bucket.minio.example.com
// - Path-style: https://minio.example.com/bucket
//
// For AWS S3, Dual stack endpoint(IPv6&IPv4) is also supported (https://docs.aws.amazon.com/AmazonS3/latest/dev/dual-stack-endpoints.html#dual-stack-endpoints-description)
// i.e. the endpoint in http://bucketname.s3.dualstack.aws-region.amazonaws.com or http://s3.dualstack.aws-region.amazonaws.com/bucketname
type S3URLParts struct {
	Scheme         string // Ex: "https://", "s3://"
	Host           string // Ex: "s3.amazonaws.com", "s3-eu-west-1.amazonaws.com", "bucket.s3-eu-west-1.amazonaws.com"
	Endpoint       string // Ex: "s3.amazonaws.com", "s3-eu-west-1.amazonaws.com"
	BucketName     string // Ex: "MyBucket"
	ObjectKey      string // Ex: "hello.txt", "foo/bar"
	Version        string
	Region         string // Ex: endpoint region, e.g. "eu-west-1"
	UnparsedParams string

	isPathStyle bool
	isDualStack bool

	ServiceType S3ServiceType
	// TODO: Extend for future work on other S3 compatible service which might be with IP endpoint style
	// Ex: http://XXX.XXX.X.X/bucketname/objectkey
}

// S3ServiceType is the specific S3 compatible storage
var ES3ServiceType = S3ServiceType(0)

type S3ServiceType uint8

func (S3ServiceType) Unknown() S3ServiceType      { return S3ServiceType(0) }
func (S3ServiceType) AWS() S3ServiceType          { return S3ServiceType(1) }
func (S3ServiceType) Wasabi() S3ServiceType       { return S3ServiceType(2) }
func (S3ServiceType) Backblaze() S3ServiceType    { return S3ServiceType(3) }
func (S3ServiceType) DigitalOcean() S3ServiceType { return S3ServiceType(4) }
func (S3ServiceType) MinIO() S3ServiceType        { return S3ServiceType(5) }

// Known S3-compatible service domains
const (
	awsDomain          = "amazonaws.com"
	wasabiDomain       = "wasabisys.com"
	backblazeDomain    = "backblazeb2.com"
	digitalOceanDomain = "digitaloceanspaces.com"
)

// detectS3ServiceType determines which S3-compatible service a URL belongs to
func detectS3ServiceType(host string) S3ServiceType {
	host = strings.ToLower(host)

	if strings.Contains(host, awsDomain) {
		return ES3ServiceType.AWS()
	} else if strings.Contains(host, wasabiDomain) {
		return ES3ServiceType.Wasabi()
	} else if strings.Contains(host, backblazeDomain) {
		return ES3ServiceType.Backblaze()
	} else if strings.Contains(host, digitalOceanDomain) {
		return ES3ServiceType.DigitalOcean()
	} else if strings.Contains(host, "minio") {
		return ES3ServiceType.MinIO()
	}
	return ES3ServiceType.Unknown()
}

const s3HostPattern = "^(?P<bucketName>.+\\.)?s3[.-](?P<dualStackOrRegionOrAWSDomain>[a-z0-9-]+)\\.(?P<regionOrAWSDomainOrCom>[a-z0-9-]+)"

/*
s3HostPatternExtended is a broader pattern matching for virtual-style and path-style URLs that follow an S3 compatible structure. Ex:
// - AWS S3: bucket.s3.amazonaws.com, s3.amazonaws.com
// - Wasabi: bucket.s3.wasabisys.com, s3.wasabisys.com
// - Backblaze: bucket.s3.backblazeb2.com, s3.backblazeb2.com
// - DigitalOcean: bucket.nyc3.digitaloceanspaces.com, nyc3.digitaloceanspaces.com
// - MinIO: bucket.minio.example.com, minio.example.com
*/
const S3HostPatternExtended = "`^(?P<bucketName>[a-z0-9][-a-z0-9.]*\\.)?(?:s3[.-](?P<dualStackOrRegion>[a-z0-9-]+)\\.)?(?P<domain>[a-z0-9][a-z0-9.-]+\\.[a-z0-9-]+)$"

const invalidS3URLErrorMessage = "invalid S3 URL. AzCopy supports standard virtual-hosted-style or path-style URLs for AWS, Wasabi, Backblaze, DigitalOcean and MinIO. Use " +
	"Set env AZCOPY_S3_COMPAT_ESSENTIAL_HOST_PART to tE.g: https://bucket.s3.amazonaws.com or https://s3.amazonaws.com/bucket"
const versionQueryParamKey = "versionId"
const s3KeywordDualStack = "dualstack"

// Default is "amazonaws.com"
var S3EssentialHostPart = strings.ToLower(GetEnvironmentVariable(EEnvironmentVariable.S3CompatSourceEssentialHostPart()))

// Returns the S3 storage keyword from the host part. Default is "amazonaws"
var s3KeywordDomain = S3EssentialHostPart[:strings.LastIndex(S3EssentialHostPart, ".")]

var s3HostRegex = regexp.MustCompile(s3HostPattern)
var s3HostRegexExtended = regexp.MustCompile(S3HostPatternExtended)

// IsS3URL verifies if a given URL points to S3 URL supported by AzCopy-v10
func IsS3URL(u url.URL) bool {
	hostParsed := strings.ToLower(u.Host)
	if _, isS3URL := findS3URLMatches(hostParsed); isS3URL {
		return true
	}
	if isMinIOHost(hostParsed) || u.Scheme == "s3" {
		return true
	}

	if strings.Contains(hostParsed, "digitaloceanspaces.com") {
		return true
	}
	return false
}

// Checks if the host is MinIO.
// Ex "http://localhost:9000" "http://minio.company.internal:9000" "s3://<bucket-name>/<object-key>"
func isMinIOHost(host string) bool {
	return strings.Contains(host, "minio") || strings.HasPrefix(host, "localhost")
}

func findS3URLMatches(host string) (matches []string, isS3Host bool) {
	matchSlices := s3HostRegex.FindStringSubmatch(host) // If match the first element would be entire host, and then follows the sub match strings.
	if matchSlices == nil || !strings.Contains(host, S3EssentialHostPart) {
		if matches := s3HostRegexExtended.FindStringSubmatch(host); matches != nil {
			return matches, true
		}
		return nil, false
	}
	return matchSlices, true
}

// NewS3URLParts parses a URL initializing S3URLParts' fields. This method overwrites all fields in the S3URLParts object.
func NewS3URLParts(u url.URL) (S3URLParts, error) {
	// S3's bucket name should be in lower case
	host := strings.ToLower(u.Host)

	matchSlices, isS3URL := findS3URLMatches(host)
	if !isS3URL {
		return S3URLParts{}, errors.New(invalidS3URLErrorMessage)
	}

	path := u.Path
	// Remove the initial '/' if exists
	if path != "" && path[0] == '/' {
		path = path[1:]
	}
	// Find the type of S3 compatible service
	serviceType := detectS3ServiceType(host)

	up := S3URLParts{
		Scheme:      u.Scheme,
		Host:        host,
		ServiceType: serviceType,
	}

	// Check what's the path style, and parse accordingly.
	if matchSlices[1] != "" { // Go's implementation is a bit strange, even if the first subexp fail to be matched, "" will be returned for that sub exp
		// In this case, it would be in virtual-hosted-style URL, and has host prefix like bucket.s3[-.]
		up.BucketName = matchSlices[1]
		if strings.HasSuffix(up.BucketName, ".") {
			up.BucketName = up.BucketName[:len(matchSlices[1])-1] // Removing the trailing '.' at the end
		}
		up.ObjectKey = path

		up.Endpoint = host[strings.Index(host, ".")+1:]
	} else {
		// In this case, it would be in path-style URL. Host prefix like s3[-.], and path contains the bucket name and object id.
		up.isPathStyle = true

		if bucketEndIndex := strings.Index(path, "/"); bucketEndIndex != -1 {
			up.BucketName = path[:bucketEndIndex]
			up.ObjectKey = path[bucketEndIndex+1:]
		} else {
			up.BucketName = path
		}

		up.Endpoint = host
	}
	// Extract region informatoin
	// Check if dualstack is contained in host name
	if matchSlices[2] != "" {
		if matchSlices[2] == s3KeywordDualStack {
			up.isDualStack = true
			if matchSlices[3] != s3KeywordDomain {
				up.Region = matchSlices[3]
			}
		} else if matchSlices[2] != s3KeywordDomain {
			up.Region = matchSlices[2]
		}

	}
	// Convert the query parameters to a case-sensitive map & trim whitespace
	paramsMap := u.Query()

	if versionStr, ok := caseInsensitiveValues(paramsMap).Get(versionQueryParamKey); ok {
		up.Version = versionStr[0]
		// If we recognized the query parameter, remove it from the map
		delete(paramsMap, versionQueryParamKey)
	}

	up.UnparsedParams = paramsMap.Encode()

	return up, nil
}

// URL returns a URL object whose fields are initialized from the S3URLParts fields.
func (p *S3URLParts) URL() url.URL {
	path := ""

	// Concatenate container & blob names (if they exist)
	if p.BucketName != "" {
		if p.isPathStyle {
			path += "/" + p.BucketName
		}
		if p.ObjectKey != "" {
			path += "/" + p.ObjectKey
		}
	}

	rawQuery := p.UnparsedParams

	if p.Version != "" {
		if len(rawQuery) > 0 {
			rawQuery += "&"
		}
		rawQuery += versionQueryParamKey + "=" + p.Version
	}
	u := url.URL{
		Scheme:   p.Scheme,
		Host:     p.Host,
		Path:     path,
		RawQuery: rawQuery,
	}
	return u
}

func (p *S3URLParts) String() string {
	u := p.URL()
	return u.String()
}

func (p *S3URLParts) IsServiceSyntactically() bool {
	if p.Host != "" && p.BucketName == "" {
		return true
	}
	return false
}

func (p *S3URLParts) IsBucketSyntactically() bool {
	if p.BucketName != "" && p.ObjectKey == "" {
		return true
	}
	return false
}

func (p *S3URLParts) IsObjectSyntactically() bool {
	if p.ObjectKey != "" {
		return true
	}
	return false
}

// IsDirectorySyntactically validates if the S3URLParts is indicating a directory.
// Note: directory in S3 is a virtual abstract, and a object as well.
func (p *S3URLParts) IsDirectorySyntactically() bool {
	if p.IsObjectSyntactically() && strings.HasSuffix(p.ObjectKey, "/") {
		return true
	}
	return false
}

type caseInsensitiveValues url.Values // map[string][]string
func (values caseInsensitiveValues) Get(key string) ([]string, bool) {
	key = strings.ToLower(key)
	for k, v := range values {
		if strings.ToLower(k) == key {
			return v, true
		}
	}
	return []string{}, false
}
