package azcopy

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

//For GCS, container naming follows:
//1. Bucket names must contain only lowercase letters, numbers, dashes (-), underscores (_), and dots (.).
//Spaces are not allowed. Names containing dots require verification.
//2. Bucket names must start and end with a number or letter.
//3. Bucket names must contain 3-63 characters. Names containing dots can contain up to 222 characters,
//but each dot-separated component can be no longer than 63 characters.
//4. Bucket names cannot be represented as an IP address in dotted-decimal notation (for example, 192.168.5.4).
//5. Bucket names cannot begin with the "goog" prefix.
//6. Bucket names cannot contain "google" or close misspellings, such as "g00gle".
//For Azure, container/filesystem/share's naming follows:
//1. Lower case letters, numbers and hyphen.
//2. 3-63 length.
//3. Name should not contain two consecutive hyphens.
//4. Name should not start or end with hyphen.
//In general we only have to fix the following cases:
//1. Names with underscores (_). Here we will try to replace it with hyphens (-).
// e.g. bucket_name -> bucket-name
//2. Names with consecutive hyphens. Here we will replace it with -[number of hyphens]-.
// e.g. bucket-----name -> bucket-5-name.
//3. Names with period (.). Here we will replace it with hyphen (-).
//4. In case of any naming collisions we try to add a suffix.
// e.g. Bucket names: bucket-name, bucket.name. Then we will resolve into bucket.name -> bucket-name -> bucket-name-2

type GCPBucketNameToAzureResourcesResolver struct {
	bucketNameResolvingMap map[string]string

	collisionDetectionMap map[string]struct{}
}

const gcpBucketNameMaxLength = 63

var gcpBucketNameResolveError = "fail to resolve GCP bucket name"

func NewGCPBucketNameToAzureResourcesResolver(gcpBucketNames []string) *GCPBucketNameToAzureResourcesResolver {
	gcpResolver := GCPBucketNameToAzureResourcesResolver{
		bucketNameResolvingMap: make(map[string]string),
		collisionDetectionMap:  make(map[string]struct{}),
	}

	for _, bucketName := range gcpBucketNames {
		gcpResolver.bucketNameResolvingMap[bucketName] = ""
	}

	for _, bucketName := range gcpBucketNames {
		_, _ = gcpResolver.ResolveName(bucketName)
	}
	return &gcpResolver
}

func (resolver *GCPBucketNameToAzureResourcesResolver) ResolveName(bucketName string) (string, error) {
	if resolvedName, ok := resolver.bucketNameResolvingMap[bucketName]; !ok || resolvedName == "" {
		resolver.bucketNameResolvingMap[bucketName] = bucketName
		resolver.resolveNewBucketNameInternal(bucketName)
		return resolver.ResolveName(bucketName)
	} else if resolvedName == failToResolveMapValue {
		return "", fmt.Errorf("%s: container name %q is invalid for destination", gcpBucketNameResolveError, bucketName)
	} else {
		return resolvedName, nil
	}
}

func (resolver *GCPBucketNameToAzureResourcesResolver) resolveNewBucketNameInternal(bucketName string) {
	hasPeriod := strings.Contains(bucketName, ".")
	hasUnderscores := strings.Contains(bucketName, "_")
	hasConsecutiveHyphens := strings.Contains(bucketName, "--")

	if !hasPeriod && !hasUnderscores && !hasConsecutiveHyphens {
		return
	}
	resolvedName := bucketName

	//Try to replace (.) with (-)
	if hasPeriod {
		resolvedName = strings.Replace(resolvedName, ".", "-", -1)
	}

	//Try to replace (_) with (-)
	if hasUnderscores {
		resolvedName = strings.Replace(resolvedName, "_", "-", -1)
	}

	hasConsecutiveHyphens = strings.Contains(resolvedName, "--")

	//Try to replace consecutive hyphens (--) with number of hyphens
	if hasConsecutiveHyphens {
		var buffer bytes.Buffer
		consecutiveHyphenCount := 0
		for i := 0; i < len(resolvedName); i++ {
			charAtI := resolvedName[i]
			if charAtI == '-' {
				consecutiveHyphenCount++
				continue
			}

			if consecutiveHyphenCount == 0 {
				buffer.WriteByte(charAtI)
			} else if consecutiveHyphenCount == 1 {
				buffer.WriteString("-")
				buffer.WriteByte(charAtI)
			} else {
				buffer.WriteString("-")
				buffer.WriteString(strconv.Itoa(consecutiveHyphenCount))
				buffer.WriteString("-")
				buffer.WriteByte(charAtI)
			}
			consecutiveHyphenCount = 0
		}
		if consecutiveHyphenCount > 0 {
			panic("Invalid value for consecutiveHyphenCount. Value cannot be greater than 0")
		}
		resolvedName = buffer.String()
	}
	if resolver.hasCollision(resolvedName) {
		resolvedName = resolver.addSuffix(resolvedName)
	}

	if !validateResolvedNameGCP(resolvedName) {
		resolvedName = failToResolveMapValue
	}
	resolver.bucketNameResolvingMap[bucketName] = resolvedName
	resolver.collisionDetectionMap[resolvedName] = struct{}{}
}

func (resolver *GCPBucketNameToAzureResourcesResolver) hasCollision(name string) bool {
	_, hasCollisionToOrgNames := resolver.bucketNameResolvingMap[name]
	_, hasCollisionToResolvedNames := resolver.collisionDetectionMap[name]
	return hasCollisionToOrgNames || hasCollisionToResolvedNames
}

func validateResolvedNameGCP(name string) bool {
	return len(name) <= gcpBucketNameMaxLength
}

func (resolver *GCPBucketNameToAzureResourcesResolver) addSuffix(bucketName string) string {
	suffixPattern := "%s-%d"

	count := 2
	resolvedName := fmt.Sprintf(suffixPattern, bucketName, count)

	for {
		if !resolver.hasCollision(resolvedName) {
			break
		}

		count++
		resolvedName = fmt.Sprintf(suffixPattern, bucketName, count)
	}
	return resolvedName
}
