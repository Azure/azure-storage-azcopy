// Copyright © 2018 Microsoft <wastore@microsoft.com>
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

package cmd

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

// S3URLParts represents the components that make up AWS S3 Service/Bucket/Object URL.
// You parse an existing URL into its parts by calling NewS3URLParts().
// According to http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro and
// https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region,
// S3URLParts supports virtual-hosted-style and path-style URL:
// Ex, virtual-hosted-style(the bucket name is part of the domain name in the URL) :
// a. http://bucket.s3.amazonaws.com
// b. http://bucket.s3-aws-region.amazonaws.com
// Ex,  path-style URL(the bucket name is not part of the domain (unless you use a Region-specific endpoint)):
// a. http://s3.amazonaws.com/bucket (US East (N. Virginia) Region endpoint)
// b. http://s3-aws-region.amazonaws.com/bucket (Region-specific endpoint)
// By design, doesn't support the dual stack endpoint(IPv6&IPv4) (https://docs.aws.amazon.com/AmazonS3/latest/dev/dual-stack-endpoints.html#dual-stack-endpoints-description)
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
	// TODO: Presigned request queries
	// TODO: Other S3 compatible service which might be with IP endpoint style
}

const s3HostPattern = "^(?P<bucketname>.+\\.)?s3[.-](?P<regionorawsdomain>[a-z0-9-]+)\\."
const invalidS3URLErrorMessage = "Invalid S3 URL. Support standard virtual-hosted-style or path-style URL defined by AWS, E.g: https://bucket.s3.amazonaws.com or https://s3.amazonaws.com/bucket"
const versionQueryParamKey = "versionId"
const s3AWSDomain = "amazonaws"

// IsS3URL verfies if a given URL points to S3 URL supported by AzCopy-v10
func IsS3URL(u url.URL) bool {
	if match, _ := regexp.MatchString(s3HostPattern, strings.ToLower(u.Host)); match {
		return true
	}
	return false
}

// NewS3URLParts parses a URL initializing S3URLParts' fields. This method overwrites all fields in the S3URLParts object.
func NewS3URLParts(u url.URL) (S3URLParts, error) {
	// Check if it's S3URL, and get essential info from URL's host
	r, err := regexp.Compile(s3HostPattern)
	if err != nil {
		return S3URLParts{}, err
	}

	// S3's bucket name should be in lower case
	host := strings.ToLower(u.Host)

	matchSlices := r.FindStringSubmatch(host) // If match the first element would be entire host, and then follows the sub match strings.
	// If matchSlices equals nil, means no match found
	if matchSlices == nil {
		return S3URLParts{}, errors.New(invalidS3URLErrorMessage)
	}

	path := u.Path
	// Remove the initial '/' if exists
	if path != "" && path[0] == '/' {
		path = path[1:]
	}

	up := S3URLParts{
		Scheme: u.Scheme,
		Host:   host,
	}

	// Check what's the path style, and parse accordingly.
	if matchSlices[1] != "" { // Go's implementatoin is a bit strange, even if the first subexp fail to be matched, "" will be returned for that sub exp
		// In this case, it would be in virtual-hosted-style URL, and has host prefix like bucket.s3[-.]
		up.BucketName = matchSlices[1][:len(matchSlices[1])-1] // Removing the trailing '.' at the end
		up.ObjectKey = path

		up.Endpoint = host[strings.Index(host, ".")+1:]
	} else {
		// In this case, it would be in path-style URL. Host prefix like s3[-.], and path contains the bucket name and object id.
		if bucketEndIndex := strings.Index(path, "/"); bucketEndIndex != -1 {
			up.BucketName = path[:bucketEndIndex]
			up.ObjectKey = path[bucketEndIndex+1:]
		} else {
			up.BucketName = path
		}

		up.Endpoint = host
	}
	// Check if region is contained in host name
	if matchSlices[2] != s3AWSDomain {
		up.Region = matchSlices[2]
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

func (p *S3URLParts) IsServiceURL() bool {
	if p.Host != "" && p.BucketName == "" {
		return true
	}
	return false
}

func (p *S3URLParts) IsBucketURL() bool {
	if p.BucketName != "" && p.ObjectKey == "" {
		return true
	}
	return false
}

func (p *S3URLParts) IsObject() bool {
	if p.ObjectKey != "" {
		return true
	}
	return false
}

// IsDirectory validates if the S3URLParts is indicating a directory.
// Note: directory in S3 is a virtual abstract, and a object as well.
func (p *S3URLParts) IsDirectory() bool {
	if p.IsObject() && strings.HasSuffix(p.ObjectKey, "/") {
		return true
	}
	return false
}

// IsServiceLevelSearch check if it's an service level search for S3.
// And returns search prefix(part before wildcard) for bucket to match, if it's service level search.
func (p *S3URLParts) IsServiceLevelSearch() (IsServiceLevelSearch bool, bucketPrefix string) {
	// If it's service level URL which need search bucket, there could be two cases:
	// a. https://<service-endpoint>(/)
	// b. https://<service-endpoint>/bucketprefix*(/*)
	if p.IsServiceURL() ||
		strings.Contains(p.BucketName, wildCard) {
		IsServiceLevelSearch = true
		// Case p.IsServiceURL(), bucket name is empty, search for all buckets.
		if p.BucketName == "" {
			return
		}

		// Case bucketname contains wildcard.
		wildCardIndex := gCopyUtil.firstIndexOfWildCard(p.BucketName)

		// wild card exists prefix will be the content of bucket name till the wildcard index
		// Example 1: for URL https://<service-endpoint>/b-2*, bucketPrefix = b-2
		// Example 2: for URL https://<service-endpoint>/b-2*/vd/o*, bucketPrefix = b-2
		bucketPrefix = p.BucketName[:wildCardIndex]
		return
	}
	// Otherwise, it's not service level search.
	return
}

// searchObjectPrefixAndPatternFromS3URL gets search prefix and pattern from S3 URL.
// search prefix is used during listing objects in bucket, and pattern is used to support wildcard search by azcopy-v10.
func (p *S3URLParts) searchObjectPrefixAndPatternFromS3URL() (prefix, pattern string, isWildcardSearch bool) {
	// If the objectKey is empty, it means the url provided is of a bucket,
	// then all object inside buckets needs to be included, so prefix is "" and pattern is set to *, isWildcardSearch false
	if p.ObjectKey == "" {
		pattern = "*"
		return
	}
	// Check for wildcard
	wildCardIndex := gCopyUtil.firstIndexOfWildCard(p.ObjectKey)
	// If no wildcard exits and url represents a virtual directory or a object, search everything in the virtual directory
	// or specifically the object.
	if wildCardIndex < 0 {
		// prefix is the path of virtual directory after the bucket, pattern is *, isWildcardSearch false
		// Example 1: https://<bucket-name>/vd-1/, prefix = /vd-1/
		// Example 2: https://<bucket-name>/vd-1/vd-2/, prefix = /vd-1/vd-2/
		// Example 3: https://<bucket-name>/vd-1/abc, prefix = /vd1/abc
		prefix = p.ObjectKey
		pattern = "*"
		return
	}

	// Is wildcard search
	isWildcardSearch = true
	// wildcard exists prefix will be the content of object key till the wildcard index
	// Example: https://<bucket-name>/vd-1/vd-2/abc*
	// prefix = /vd-1/vd-2/abc, pattern = /vd-1/vd-2/abc*, isWildcardSearch true
	prefix = p.ObjectKey[:wildCardIndex]
	pattern = p.ObjectKey

	return
}

// Get the source path without the wildcards
// This is defined since the files mentioned with exclude flag
// & include flag are relative to the Source
// If the source has wildcards, then files are relative to the
// parent source path which is the path of last directory in the source
// without wildcards
// For Example: src = "/home/user/dir1" parentSourcePath = "/home/user/dir1"
// For Example: src = "/home/user/dir*" parentSourcePath = "/home/user"
// For Example: src = "/home/*" parentSourcePath = "/home"
func (p *S3URLParts) getParentSourcePath() string {
	parentSourcePath := p.ObjectKey
	wcIndex := gCopyUtil.firstIndexOfWildCard(parentSourcePath)
	if wcIndex != -1 {
		parentSourcePath = parentSourcePath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentSourcePath, "/")
		if pathSepIndex == -1 {
			parentSourcePath = ""
		} else {
			parentSourcePath = parentSourcePath[:pathSepIndex]
		}
	}

	return parentSourcePath
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
