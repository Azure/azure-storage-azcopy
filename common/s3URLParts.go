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
	"os"
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
// Dual stack endpoint(IPv6&IPv4) is also supported (https://docs.aws.amazon.com/AmazonS3/latest/dev/dual-stack-endpoints.html#dual-stack-endpoints-description)
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
	// TODO: Other S3 compatible service which might be with IP endpoint style
}

const s3HostPattern = "^(?P<bucketName>.+\\.)?s3[.-](?P<dualStackOrRegionOrAWSDomain>[a-z0-9-]+)\\.(?P<regionOrAWSDomainOrCom>[a-z0-9-]+)"
const invalidS3URLErrorMessage = "Invalid S3 URL. AzCopy supports standard virtual-hosted-style or path-style URLs defined by AWS, E.g: https://bucket.s3.amazonaws.com or https://s3.amazonaws.com/bucket"
const versionQueryParamKey = "versionId"
const s3KeywordAmazonAWS = "amazonaws"
const s3KeywordDualStack = "dualstack"
const s3EssentialHostPart = "amazonaws.com"

var s3HostRegex = regexp.MustCompile(s3HostPattern)

func GetS3CompatibleSuffix() string {
	if os.Getenv("S3_COMPATIBLE_ENDPOINT") != "" {
		return os.Getenv("S3_COMPATIBLE_ENDPOINT")
	} else {
		return "amazonaws.com"
	}
}

func getS3Keyword() string {
	suffix := strings.ToLower(GetS3CompatibleSuffix())

	switch {
	case strings.HasSuffix(suffix, "oraclecloud.com"), strings.HasSuffix(suffix, "oci.customer-oci.com"):
		return "oracle"
	case strings.HasSuffix(suffix, "googleapis.com"):
		return "googleapis"
	case strings.HasSuffix(suffix, "amazonaws.com"):
		return "amazonaws"
	default:
		return "custom"
	}
}

// IsS3URL verifies if a given URL points to S3 URL supported by AzCopy-v10
func IsS3URL(u url.URL) bool {
	if _, isS3URL := findS3URLMatches(strings.ToLower(u.Host)); isS3URL {
		return true
	}
	return false
}

// findS3URLMatches identifies whether the given host corresponds to an S3 (or S3-compatible) endpoint
// and returns a synthesized slice similar to the AWS regex submatches:
//
//	[ fullHost, bucketCapture(with trailing '.' if present OR ""), regionOrDualStack, keywordDomainRoot ]
//
// For path-style compatible providers where bucket is not in host, bucketCapture is "" so caller treats it as path-style.
// The host should be passed as lower-case for consistent matching.
func findS3URLMatches(host string) (matches []string, isS3Host bool) {
	suffix := GetS3CompatibleSuffix()
	hostLower := strings.ToLower(host)

	// Dispatcher based on configured suffix (allows per-provider parsing differences)
	switch {
	case strings.HasSuffix(hostLower, "."+suffix) || hostLower == suffix:
		// Pick provider-specific matcher using well-known suffixes
		switch {
		case suffix == "amazonaws.com":
			if m := matchAWSHost(hostLower, suffix); m != nil {
				return m, true
			}
		case strings.HasSuffix(suffix, "oraclecloud.com"), strings.HasSuffix(suffix, "oci.customer-oci.com"):
			if m := matchOCIHost(hostLower, suffix); m != nil {
				return m, true
			}
		case strings.HasSuffix(suffix, "googleapis.com"):
			// Always pass the domain root so matchGoogleHost builds correct endpoints
			// (e.g. suffix may be "storage.googleapis.com" but matcher needs "googleapis.com")
			if m := matchGoogleHost(hostLower, "googleapis.com"); m != nil {
				return m, true
			}
		default:
			if m := matchGoogleHost(hostLower, suffix); m != nil {
				return m, true
			}
			if m := matchAWSHost(hostLower, suffix); m != nil {
				return m, true
			}
			if m := matchOCIHost(hostLower, suffix); m != nil {
				return m, true
			}
			// For custom (non-well-known) suffixes, allow any valid FQDN
			// for on-prem S3-compatible appliances (e.g., MinIO, NetApp, Dell EMC, etc.)
			if os.Getenv("S3_COMPATIBLE_ENDPOINT") != "" && IsPrivateNetworkTransfer(ELocation.S3()) {
				if m := matchCustomS3Host(hostLower); m != nil {
					return m, true
				}
			}
		}
	}

	return nil, false
}

// matchAWSHost uses the legacy AWS regex; requires suffix to be present.
func matchAWSHost(hostLower, suffix string) []string {
	if !strings.Contains(hostLower, suffix) {
		return nil
	}
	matchSlices := s3HostRegex.FindStringSubmatch(hostLower)
	if matchSlices == nil {
		return nil
	}
	// Ensure suffix presence (already checked) then return
	return matchSlices
}

// matchGoogleHost matches Google Cloud Storage endpoints in four forms:
//   - Path-style global:    storage.googleapis.com/bucketName
//   - Path-style regional:  storage.<region>.rep.googleapis.com/bucketName
//   - Path-style PSC:       storage-<psc_name>.p.googleapis.com/bucketName
//   - Virtual-hosted style: bucketName.storage.googleapis.com
//
// Bucket and object come from the path for path-style, or from the host prefix for virtual-hosted.
func matchGoogleHost(hostLower, suffix string) []string {
	keyword := getS3Keyword() // googleapis

	// Case 1: Global path-style: storage.googleapis.com
	globalEndpoint := "storage." + suffix
	if hostLower == globalEndpoint {
		return []string{hostLower, "", "", keyword}
	}

	// Case 2: Regional path-style: storage.<region>.rep.googleapis.com
	repSuffix := ".rep." + suffix
	storagePrefix := "storage."
	if strings.HasPrefix(hostLower, storagePrefix) && strings.HasSuffix(hostLower, repSuffix) {
		region := hostLower[len(storagePrefix) : len(hostLower)-len(repSuffix)]
		if region != "" {
			return []string{hostLower, "", region, keyword}
		}
	}

	// Case 3: PSC path-style: storage-<psc_id>.p.googleapis.com
	// Private Service Connect endpoints use this format with no region.
	pscSuffix := ".p." + suffix
	if strings.HasPrefix(hostLower, "storage-") && strings.HasSuffix(hostLower, pscSuffix) {
		pscID := hostLower[len("storage-") : len(hostLower)-len(pscSuffix)]
		if pscID != "" {
			return []string{hostLower, "", "", keyword}
		}
	}

	// Case 4: Virtual-hosted style: <bucket>.storage.googleapis.com
	if strings.HasSuffix(hostLower, "."+globalEndpoint) {
		bucketWithDot := hostLower[:len(hostLower)-len(globalEndpoint)] // includes trailing "."
		if bucketWithDot != "" {
			return []string{hostLower, bucketWithDot, "", keyword}
		}
	}

	return nil
}

// matchCustomS3Host handles arbitrary FQDN hosts for on-prem S3-compatible appliances.
// This supports custom domains like s3.company.com, minio.internal.net, storage.local, etc.
// Assumes path-style URLs (bucket in path, not subdomain) for maximum compatibility.
func matchCustomS3Host(hostLower string) []string {
	if hostLower == "" {
		return nil
	}

	configuredHost := strings.ToLower(os.Getenv("S3_COMPATIBLE_ENDPOINT"))
	if configuredHost == "" {
		return nil
	}

	keyword := getS3Keyword()
	region := ""

	// Path-style: exact endpoint host
	if hostLower == configuredHost {
		return []string{hostLower, "", region, keyword}
	}

	// Virtual-hosted style: <bucket>.<configuredHost>
	suffix := "." + configuredHost
	if strings.HasSuffix(hostLower, suffix) {
		bucketPart := strings.TrimSuffix(hostLower, suffix)
		if bucketPart != "" && !strings.Contains(bucketPart, "..") {
			return []string{hostLower, bucketPart + ".", region, keyword}
		}
	}

	return nil
}

func parseOCIPathStyleHost(hostLower string) (region string, ok bool) {
	markers := []string{
		".compat.objectstorage.",
		".private.compat.objectstorage.",
	}
	domainSuffixes := []string{
		".oraclecloud.com",
		".oci.customer-oci.com",
	}

	for _, marker := range markers {
		for _, domainSuffix := range domainSuffixes {
			if !strings.HasSuffix(hostLower, domainSuffix) {
				continue
			}

			markerIndex := strings.Index(hostLower, marker)
			if markerIndex <= 0 {
				continue
			}

			prefix := hostLower[:markerIndex]
			if prefix == "" {
				continue
			}

			regionStart := markerIndex + len(marker)
			regionEnd := len(hostLower) - len(domainSuffix)
			if regionStart >= regionEnd {
				continue
			}

			region = hostLower[regionStart:regionEnd]
			if region != "" {
				return region, true
			}
		}
	}

	return "", false
}

func parseOCIVirtualHostedHost(hostLower string) (bucketName string, region string, ok bool) {
	const marker = ".vhcompat.objectstorage."
	domainSuffixes := []string{
		".oraclecloud.com",
		".oci.customer-oci.com",
	}

	for _, domainSuffix := range domainSuffixes {
		if !strings.HasSuffix(hostLower, domainSuffix) {
			continue
		}

		markerIndex := strings.Index(hostLower, marker)
		if markerIndex <= 0 {
			continue
		}

		bucketName = hostLower[:markerIndex]
		if bucketName == "" {
			continue
		}

		regionStart := markerIndex + len(marker)
		regionEnd := len(hostLower) - len(domainSuffix)
		if regionStart >= regionEnd {
			continue
		}

		region = hostLower[regionStart:regionEnd]
		if region != "" {
			return bucketName, region, true
		}
	}

	return "", "", false
}

// matchOCIHost matches OCI S3-compatible endpoints in these formats:
//
//	https://<namespace>.compat.objectstorage.<region>.oraclecloud.com/<bucket-name>/<object-name>
//	https://<namespace>.compat.objectstorage.<region>.oci.customer-oci.com/<bucket-name>/<object-name>
//	https://<bucket-name>.vhcompat.objectstorage.<region>.oci.customer-oci.com/<object-name>
//	https://<dns-prefix>-<namespace>.private.compat.objectstorage.<region>.oci.customer-oci.com/<bucket-name>/<object-name>
//
// Returns a matches slice in the format: [fullHost, "", region, keyword]
func matchOCIHost(hostLower, suffix string) []string {
	if hostLower == "" || suffix == "" {
		return nil
	}

	if bucketName, region, ok := parseOCIVirtualHostedHost(hostLower); ok {
		keyword := getS3Keyword()
		return []string{hostLower, bucketName + ".", region, keyword}
	}

	region, ok := parseOCIPathStyleHost(hostLower)
	if !ok {
		return nil
	}

	keyword := getS3Keyword()

	// Return matches in the format expected by the parser.
	// [fullHost, bucketCapture (empty for path-style), region, keyword]
	return []string{hostLower, "", region, keyword}
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

	up := S3URLParts{
		Scheme: u.Scheme,
		Host:   host,
	}

	// Check what's the path style, and parse accordingly.
	if matchSlices[1] != "" { // Go's implementation is a bit strange, even if the first subexp fail to be matched, "" will be returned for that sub exp
		// In this case, it would be in virtual-hosted-style URL, and has host prefix like bucket.s3[-.]
		up.BucketName = matchSlices[1][:len(matchSlices[1])-1] // Removing the trailing '.' at the end
		up.ObjectKey = path

		up.Endpoint = host[strings.Index(host, ".")+1:]
	} else {
		// In this case, it would be in path-style URL. Host prefix like s3[-.], and path contains the bucket name and object id.
		up.isPathStyle = true

		// Standard path-style: bucket/object
		if bucketEndIndex := strings.Index(path, "/"); bucketEndIndex != -1 {
			up.BucketName = path[:bucketEndIndex]
			up.ObjectKey = path[bucketEndIndex+1:]
		} else {
			up.BucketName = path
		}

		up.Endpoint = host
	}

	// For S3-compatible endpoints (e.g. GCS path-style), the ObjectKey may have a
	// leading "/" after URL parsing which causes a double-slash in minio's path-style
	// request URL (bucket//objectKey), resulting in 403 errors.
	if up.IsS3CompatibleEndpoint() && strings.HasPrefix(up.ObjectKey, "/") {
		up.ObjectKey = strings.TrimLeft(up.ObjectKey, "/")
	}

	// Check if dualstack is contained in host name
	s3KeywordAmazonAWS := getS3Keyword()
	if matchSlices[2] == s3KeywordDualStack {
		up.isDualStack = true
		if matchSlices[3] != s3KeywordAmazonAWS {
			up.Region = matchSlices[3]
		}
	} else if matchSlices[2] != s3KeywordAmazonAWS {
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

// IsGoogleCloudStorage checks if this S3 URL is actually pointing to Google Cloud Storage
// (via S3-compatible API with HMAC keys). Returns true if the endpoint is storage.googleapis.com.
func (p *S3URLParts) IsGoogleCloudStorage() bool {
	return strings.Contains(strings.ToLower(p.Endpoint), "googleapis.com")
}

// IsOracleCloudStorage checks if this S3 URL is actually pointing to Oracle Cloud Infrastructure (OCI)
// Object Storage (via S3-compatible API). Returns true for oraclecloud.com or customer-oci.com endpoints.
func (p *S3URLParts) IsOracleCloudStorage() bool {
	endpoint := strings.ToLower(p.Endpoint)
	return strings.Contains(endpoint, "oraclecloud.com") || strings.Contains(endpoint, "oci.customer-oci.com")
}

func (p *S3URLParts) IsOracleCloudStorageVirtualHosted() bool {
	endpoint := strings.ToLower(p.Endpoint)
	return strings.HasPrefix(endpoint, "vhcompat.objectstorage.") || strings.Contains(endpoint, ".vhcompat.objectstorage.")
}

// IsS3CompatibleEndpoint returns true if a custom S3-compatible endpoint is configured
// via the S3_COMPATIBLE_ENDPOINT environment variable.
func (p *S3URLParts) IsS3CompatibleEndpoint() bool {
	return os.Getenv("S3_COMPATIBLE_ENDPOINT") != ""
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
