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

package cmd

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// S3BucketNameToAzureResourcesResolver resolves s3 bucket name to Azure Blob container/ADLS Gen2 filesystem/File share.
// For Azure, container/filesystem/share's naming follows:
// 1. Lower case letters, numbers and hyphen.
// 2. 3-63 length.
// 3. Name should not contain two consecutive hyphens.
// 4. Name should not start or end with hyphen.
// For S3, bucket's naming follows:
// 1. The bucket name can be between 3 and 63 characters long, and can contain only lower-case characters, numbers, periods, and dashes.
// 2. Each label in the bucket name must start with a lowercase letter or number.
// 3. The bucket name cannot contain underscores, end with a dash or period, have consecutive periods, or use dashes adjacent to periods.
// 4. The bucket name cannot be formatted as an IP address (198.51.100.24).
// Two common cases need be solved are:
// 1. bucket name with period. In this case, AzCopy try to replace period with hyphen.
// e.g. bucket.with.period -> bucket-with-period
// 2. bucket name with consecutive hyphens. In this case, AzCopy try to replace consecutive hyphen, with -[numberOfHyphens]-.
// e.g. bucket----hyphens -> bucket-4-hyphens
// The resolver checks if there are naming collision with other existing bucket names, and try to add suffix when there is any collision.
// e.g. There is buckets with name: bucket-name, bucket.name. Azcopy will resolve bucket.name -> bucket-name -> bucket-name-2
// All the resolving should be logged and warned to user.
type S3BucketNameToAzureResourcesResolver struct {
	// S3 has service limitation, that one S3 account can only have 100 buckets, except opening service ticket to increase the number.
	// Considering this limitation and the REST API to get bucket info is in unsegmented pattern, we assume AzCopy can always get all the bucket name with one request easily.
	bucketNameResolvingMap map[string]string

	// collisionDetectionMap is acutally used as a set to save resolved new keys, to avoid new key collisions.
	collisionDetectionMap map[string]struct{}
}

const s3BucketNameMaxLength = 63
const failToResolveMapValue = "<resolving_failed>"

var s3BucketNameResolveError = "fail to resolve s3 bucket name"

// NewS3BucketNameToAzureResourcesResolver creates S3BucketNameToAzureResourcesResolver.
// S3BucketNameToAzureResourcesResolver works in such pattern:
// 1. User provided all the bucket names returned in a certain time. (S3 has service limitation, that one S3 account can only have 100 buckets, except opening service ticket to increase the number.)
// 2. S3BucketNameToAzureResourcesResolver resolves the names with one logic pass during creating the resolver instance.
// 3. User can get resolved name later with ResolveName.
// As S3BucketNameToAzureResourcesResolver need to detect naming collision, the resolver doesn't accept adding new bucket name except the initial s3BucketNames,
// considering there could be future valid name that is not predictable during previous naming resolving.
func NewS3BucketNameToAzureResourcesResolver(s3BucketNames []string) *S3BucketNameToAzureResourcesResolver {
	s3Resolver := S3BucketNameToAzureResourcesResolver{
		bucketNameResolvingMap: make(map[string]string),
		collisionDetectionMap:  make(map[string]struct{}),
	}

	for _, bucketName := range s3BucketNames {
		s3Resolver.bucketNameResolvingMap[bucketName] = bucketName
	}

	s3Resolver.resolveS3BucketNameToAzureResources()

	return &s3Resolver
}

// ResolveName returns resolved name for given bucket name.
func (s3Resolver *S3BucketNameToAzureResourcesResolver) ResolveName(bucketName string) (string, error) {
	if resolvedName, ok := s3Resolver.bucketNameResolvingMap[bucketName]; !ok {
		// Resolve the new bucket name, recurse.

		s3Resolver.bucketNameResolvingMap[bucketName] = bucketName
		s3Resolver.resolveNewBucketNameInternal(bucketName)

		return s3Resolver.ResolveName(bucketName)
	} else if resolvedName == failToResolveMapValue {
		return "", fmt.Errorf("%s: container name %q is invalid for the destination, and azcopy failed to convert it automatically", s3BucketNameResolveError, bucketName)
	} else {
		return resolvedName, nil
	}
}

func (s3Resolver *S3BucketNameToAzureResourcesResolver) resolveS3BucketNameToAzureResources() {
	for orgBucketName := range s3Resolver.bucketNameResolvingMap {
		s3Resolver.resolveNewBucketNameInternal(orgBucketName)
	}
}

func (s3Resolver *S3BucketNameToAzureResourcesResolver) resolveNewBucketNameInternal(orgBucketName string) {
	// Check if the bucket name contains periods or consecutive hyphens, and try to resolve them.
	hasPeriod := strings.Contains(orgBucketName, ".")
	hasConsecutiveHyphen := strings.Contains(orgBucketName, "--")

	if !hasPeriod && !hasConsecutiveHyphen {
		// The name should be valid for Azure
		return
	}

	// Init resolved name as original bucket name
	resolvedName := orgBucketName

	// 1. Try to replace period with hyphen.
	// Note: there should be no '.' adjacent to '-'.
	if hasPeriod {
		resolvedName = strings.Replace(orgBucketName, ".", "-", -1)
	}

	// 2. Try to replace consecutive hyphen with -[number]-.
	// e.g.: bucket--name will be resolved as bucket-2-name, and b---name will be resolved as b-3-name
	if hasConsecutiveHyphen {
		var buffer bytes.Buffer
		consecutiveHyphenCount := 0
		for i := 0; i < len(resolvedName); i++ {
			charAtI := resolvedName[i] // ASCII is enough for bucket name which contains lower-case characters, numbers, periods, and dashes.
			if charAtI == '-' {
				// the char is hyphen, adding consecutiveHyphenCount and continue
				consecutiveHyphenCount++
				continue
			}

			// Found byte that doesn't indicate '-'
			if consecutiveHyphenCount == 0 {
				// current char is non '-', and no preceeding '-', directly write the char to buffer.
				buffer.WriteByte(charAtI)
			} else if consecutiveHyphenCount == 1 {
				buffer.WriteString("-")
				buffer.WriteByte(charAtI)
			} else { // consecutiveHyphenCount > 1
				buffer.WriteString("-")
				buffer.WriteString(strconv.Itoa(consecutiveHyphenCount))
				buffer.WriteString("-")
				buffer.WriteByte(charAtI)
			}
			consecutiveHyphenCount = 0
		}
		// Please note S3's bucketname cannot use '-' as suffix, so don't need to handle the case where consecutiveHyphenCount is larger than 0.
		if consecutiveHyphenCount > 0 {
			panic("invalid state: consecutiveHyphenCount should not be greater than 0")
		}
		resolvedName = buffer.String()
	}

	// 3. If there is naming collision, try to add suffix.
	if s3Resolver.hasCollision(resolvedName) {
		resolvedName = s3Resolver.addSuffix(resolvedName)
	}

	// 4. Validate if name is resolved correctly.
	if !validateResolvedName(resolvedName) {
		resolvedName = failToResolveMapValue
	}

	// Save the resolved name's value
	s3Resolver.bucketNameResolvingMap[orgBucketName] = resolvedName
	s3Resolver.collisionDetectionMap[resolvedName] = struct{}{}
}

// hasCollision checkes if the given name will cause collision to existing bucket names.
func (s3Resolver *S3BucketNameToAzureResourcesResolver) hasCollision(name string) bool {
	_, hasCollisionToOrgNames := s3Resolver.bucketNameResolvingMap[name]
	_, hasCollisionToNewNames := s3Resolver.collisionDetectionMap[name]

	return hasCollisionToOrgNames || hasCollisionToNewNames
}

func validateResolvedName(name string) bool {
	if len(name) > s3BucketNameMaxLength {
		return false
	}
	return true
}

// addSuffix adds suffix in order to avoid naming collision
func (s3Resolver *S3BucketNameToAzureResourcesResolver) addSuffix(name string) string {
	suffixPattern := "%s-%d"

	count := 2 // start from 2, as there is already an existed resolved name if addSuffix is called
	resolvedName := fmt.Sprintf(suffixPattern, name, count)
	// S3 has service limitation, that one S3 account can only have 100 buckets, except opening service ticket to increase the number,
	// so the loop should finish soon.
	for {
		if !s3Resolver.hasCollision(resolvedName) {
			break
		}

		if count > 999 {
			// Currently, S3 has 100 for buckets' number per S3 account by default.
			// Considering S3's further extension, adding this defensive logic.
			resolvedName = failToResolveMapValue
			break
		}

		count++
		resolvedName = fmt.Sprintf(suffixPattern, name, count)
	}
	return resolvedName
}
