package e2etest

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func ValidatePropertyPtr[T any](a Asserter, name string, expected, real *T) {
	if expected == nil {
		return
	}

	a.Assert(name+" must match", Equal{Deep: true}, expected, real)
}

func ValidateTimePtr(a Asserter, name string, expected, real *time.Time) {
	if expected == nil {
		return
	}
	diff := real.UTC().Sub(expected.UTC()).Abs()
	withinRange := diff <= time.Second // Allow small difference for e2e testing
	a.Assert(name+" must be within 1s of each other. LMT diff: "+diff.String(), Equal{}, withinRange, true)
}

func ValidateMetadata(a Asserter, expected, real common.Metadata) {
	if expected == nil {
		return
	}

	rule := func(key string, value *string) (ok string, ov *string, include bool) {
		ov = value
		ok = strings.ToLower(key)
		include = Any(common.AllLinuxProperties, func(s string) bool {
			return strings.EqualFold(key, s)
		})

		return
	}

	//a.Assert("Metadata must match", Equal{Deep: true}, expected, real)
	expected = CloneMapWithRule(expected, rule)
}

func ValidateTags(a Asserter, expected, real map[string]string) {
	if expected == nil {
		return
	}

	a.Assert("Tags must match", Equal{Deep: true}, expected, real)
}

func ValidateSkippedSymlinksCount(a Asserter, stdOut AzCopyStdout, expected uint32) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	parsedStdout := GetTypeOrAssert[*AzCopyParsedCopySyncRemoveStdout](a, stdOut)
	skippedSymlinkedCount := parsedStdout.FinalStatus.SkippedSymlinkCount
	if skippedSymlinkedCount != expected {
		a.Error(fmt.Sprintf("expected skipped symlink count (%d) received count (%d)", expected, skippedSymlinkedCount))
	}
}

func ValidateSkippedSpecialFileCount(a Asserter, stdOut AzCopyStdout, expected uint32) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	parsedStdout := GetTypeOrAssert[*AzCopyParsedCopySyncRemoveStdout](a, stdOut)
	skippedSpecialFileCount := parsedStdout.FinalStatus.SkippedSpecialFileCount
	if skippedSpecialFileCount != expected {
		a.Error(fmt.Sprintf("expected skipped special file count (%d) received count (%d)", expected, skippedSpecialFileCount))
	}
	return
}

type ValidateResourceOptions struct {
	validateObjectContent bool
	fromTo                common.FromTo
	preservePermissions   bool
	preserveInfo          bool
}

func ValidateResource[T ResourceManager](a Asserter, target T, definition MatchedResourceDefinition[T], validateOptions ValidateResourceOptions) {
	a.AssertNow("Target resource and definition must not be null", Not{IsNil{}}, a, target, definition)
	a.AssertNow("Target resource must be at a equal level to the resource definition", Equal{}, target.Level(), definition.DefinitionTarget())

	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	definition.ApplyDefinition(a, target, map[cmd.LocationLevel]func(Asserter, ResourceManager, ResourceDefinition){
		cmd.ELocationLevel.Container(): func(a Asserter, manager ResourceManager, definition ResourceDefinition) {
			cRes := manager.(ContainerResourceManager)

			canonPathPrefix := cRes.Canon() + ": "

			if !definition.ShouldExist() {
				a.AssertNow(canonPathPrefix+"container must not exist", Equal{}, cRes.Exists(), false)
				return
			}

			cProps := cRes.GetProperties(a)
			vProps := definition.(ResourceDefinitionContainer).Properties

			ValidateMetadata(a, vProps.Metadata, cProps.Metadata)

			if manager.Location() == common.ELocation.Blob() || manager.Location() == common.ELocation.BlobFS() {
				ValidatePropertyPtr(a, canonPathPrefix+"Public access", vProps.BlobContainerProperties.Access, cProps.BlobContainerProperties.Access)
			}

			if manager.Location().IsFile() {
				ValidatePropertyPtr(a, "Enabled protocols", vProps.FileContainerProperties.EnabledProtocols, cProps.FileContainerProperties.EnabledProtocols)
				ValidatePropertyPtr(a, "RootSquash", vProps.FileContainerProperties.RootSquash, cProps.FileContainerProperties.RootSquash)
				ValidatePropertyPtr(a, "AccessTier", vProps.FileContainerProperties.AccessTier, cProps.FileContainerProperties.AccessTier)
				ValidatePropertyPtr(a, "Quota", vProps.FileContainerProperties.Quota, cProps.FileContainerProperties.Quota)
			}
		},
		cmd.ELocationLevel.Object(): func(a Asserter, manager ResourceManager, definition ResourceDefinition) {
			objMan := manager.(ObjectResourceManager)
			objDef := definition.(ResourceDefinitionObject)

			canonPathPrefix := objMan.Canon() + ": "

			if !objDef.ShouldExist() {
				a.Assert(fmt.Sprintf("object %s must not exist", objMan.ObjectName()), Equal{}, objMan.Exists(), false)
				return
			}

			oProps := objMan.GetProperties(a)
			vProps := objDef.ObjectProperties

			if validateOptions.validateObjectContent && (objMan.EntityType() == common.EEntityType.File() ||
				objMan.EntityType() == common.EEntityType.Hardlink()) && objDef.Body != nil {

				objBody := objMan.Download(a)
				validationBody := objDef.Body.Reader()

				objHash := md5.New()
				valHash := md5.New()

				_, err := io.Copy(objHash, objBody)
				a.NoError(canonPathPrefix+"hash object body", err)
				_, err = io.Copy(valHash, validationBody)
				a.NoError(canonPathPrefix+"hash validation body", err)

				a.Assert(canonPathPrefix+"bodies differ in hash", Equal{Deep: true}, hex.EncodeToString(objHash.Sum(nil)), hex.EncodeToString(valHash.Sum(nil)))
			} else if objMan.EntityType() == common.EEntityType.Symlink() {
				// NFS symlink target is stored as file content
				linkDataDest := objMan.ReadLink(a)
				linkDataSrc := objDef.SymlinkedFileName

				if linkDataDest != "" {
					linkData := objMan.ReadLink(a)

					decodedDest, err := url.PathUnescape(linkData)
					a.NoError("decode failed", err) // or handle error if needed

					// Normalize both paths for comparison
					srcBase := filepath.Base(linkDataSrc)
					destBase := filepath.Base(decodedDest)

					a.Assert(canonPathPrefix+"Symlink mismatch", Equal{}, srcBase, destBase)
				}
			}

			// properties
			ValidateMetadata(a, vProps.Metadata, oProps.Metadata)

			// HTTP headers
			ValidatePropertyPtr(a, canonPathPrefix+"Cache control", vProps.HTTPHeaders.cacheControl, oProps.HTTPHeaders.cacheControl)
			ValidatePropertyPtr(a, canonPathPrefix+"Content disposition", vProps.HTTPHeaders.contentDisposition, oProps.HTTPHeaders.contentDisposition)
			ValidatePropertyPtr(a, canonPathPrefix+"Content encoding", vProps.HTTPHeaders.contentEncoding, oProps.HTTPHeaders.contentEncoding)
			ValidatePropertyPtr(a, canonPathPrefix+"Content language", vProps.HTTPHeaders.contentLanguage, oProps.HTTPHeaders.contentLanguage)
			ValidatePropertyPtr(a, canonPathPrefix+"Content type", vProps.HTTPHeaders.contentType, oProps.HTTPHeaders.contentType)

			switch manager.Location() {
			case common.ELocation.Blob():
				ValidatePropertyPtr(a, canonPathPrefix+"Blob type", vProps.BlobProperties.Type, oProps.BlobProperties.Type)
				ValidateTags(a, vProps.BlobProperties.Tags, oProps.BlobProperties.Tags)
				ValidatePropertyPtr(a, "Block blob access tier", vProps.BlobProperties.BlockBlobAccessTier, oProps.BlobProperties.BlockBlobAccessTier)
				ValidatePropertyPtr(a, "Page blob access tier", vProps.BlobProperties.PageBlobAccessTier, oProps.BlobProperties.PageBlobAccessTier)
			case common.ELocation.File(), common.ELocation.FileNFS():
				ValidatePropertyPtr(a, "Attributes", vProps.FileProperties.FileAttributes, oProps.FileProperties.FileAttributes)
				ValidatePropertyPtr(a, "Permissions", vProps.FileProperties.FilePermissions, oProps.FileProperties.FilePermissions)

				if validateOptions.preserveInfo && validateOptions.fromTo == common.EFromTo.FileSMBFileNFS() { // SMB to NFS transfer
					ValidateTimePtr(a, "Creation time SMB to NFS", vProps.FileProperties.FileCreationTime, oProps.FileNFSProperties.FileCreationTime)
					ValidateTimePtr(a, "Last write time SMB to NFS", vProps.FileProperties.FileLastWriteTime, oProps.FileNFSProperties.FileLastWriteTime)

				} else if validateOptions.preserveInfo && validateOptions.fromTo == common.EFromTo.FileNFSFileSMB() { // NFS to SMB transfer
					ValidateTimePtr(a, "Creation time NFS to SMB", vProps.FileNFSProperties.FileCreationTime, oProps.FileProperties.FileCreationTime)
					ValidateTimePtr(a, "Last write time NFS to SMB", vProps.FileNFSProperties.FileLastWriteTime, oProps.FileProperties.FileLastWriteTime)

				} else if validateOptions.preserveInfo && validateOptions.fromTo == common.EFromTo.FileSMBFileSMB() { // SMB to SMB transfer
					ValidateTimePtr(a, "Creation time SMB to SMB", vProps.FileProperties.FileCreationTime, oProps.FileProperties.FileCreationTime)
					ValidateTimePtr(a, "Last write time SMB to SMB", vProps.FileProperties.FileLastWriteTime, oProps.FileProperties.FileLastWriteTime)

				} else if validateOptions.preserveInfo && validateOptions.fromTo == common.EFromTo.FileNFSFileNFS() { // NFS to NFS transfers
					ValidateTimePtr(a, canonPathPrefix+"Creation Time NFS to NFS", vProps.FileNFSProperties.FileCreationTime, oProps.FileNFSProperties.FileCreationTime)
					ValidateTimePtr(a, canonPathPrefix+"Last Write Time NFS to NFS", vProps.FileNFSProperties.FileLastWriteTime, oProps.FileNFSProperties.FileLastWriteTime)

				}
				if validateOptions.preservePermissions {
					ValidatePropertyPtr(a, canonPathPrefix+"Owner", vProps.FileNFSPermissions.Owner, oProps.FileNFSPermissions.Owner)
					ValidatePropertyPtr(a, canonPathPrefix+"Group", vProps.FileNFSPermissions.Group, oProps.FileNFSPermissions.Group)
					// On Linux, symlink mode bits are mostly ignored by the kernel.
					// By default, symlinks are created with 0777, and you cannot change
					// their mode with chmod — the syscall always succeeds but doesn’t alter them.
					if objMan.EntityType() != common.EEntityType.Symlink() {
						ValidatePropertyPtr(a, canonPathPrefix+"FileMode", vProps.FileNFSPermissions.FileMode, oProps.FileNFSPermissions.FileMode)
					}
				}
			case common.ELocation.BlobFS():
				ValidatePropertyPtr(a, canonPathPrefix+"Permissions", vProps.BlobFSProperties.Permissions, oProps.BlobFSProperties.Permissions)
				ValidatePropertyPtr(a, canonPathPrefix+"Owner", vProps.BlobFSProperties.Owner, oProps.BlobFSProperties.Owner)
				ValidatePropertyPtr(a, canonPathPrefix+"Group", vProps.BlobFSProperties.Group, oProps.BlobFSProperties.Group)
				ValidatePropertyPtr(a, canonPathPrefix+"ACL", vProps.BlobFSProperties.ACL, oProps.BlobFSProperties.ACL)
			case common.ELocation.Local():
				ValidateTimePtr(a, canonPathPrefix+"Last modified time", vProps.LastModifiedTime, oProps.LastModifiedTime)
			}
		},
	})
}

type AzCopyOutputKey struct {
	Path       string
	VersionId  string
	SnapshotId string
}

func ValidateListOutput(a Asserter, stdout AzCopyStdout, expectedObjects map[AzCopyOutputKey]cmd.AzCopyListObject, expectedSummary *cmd.AzCopyListSummary) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	listStdout, ok := stdout.(*AzCopyParsedListStdout)
	a.AssertNow("stdout must be AzCopyParsedListStdout", Equal{}, ok, true)

	a.AssertNow("stdout and expected objects must not be null", Not{IsNil{}}, a, stdout, expectedObjects)
	a.Assert("map of objects must be equivalent in size", Equal{}, len(expectedObjects), len(listStdout.Items))
	a.Assert("map of objects must match", MapContains[AzCopyOutputKey, cmd.AzCopyListObject]{TargetMap: expectedObjects}, listStdout.Items)
	a.Assert("summary must match", Equal{}, listStdout.Summary, DerefOrZero(expectedSummary))
}

func ValidateHardlinksConvertedCount(a Asserter, stdOut AzCopyStdout, expected uint32) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	parsedStdout := GetTypeOrAssert[*AzCopyParsedCopySyncRemoveStdout](a, stdOut)
	hardlinksConvertedCount := parsedStdout.FinalStatus.HardlinksConvertedCount
	if hardlinksConvertedCount != expected {
		a.Error(fmt.Sprintf("expected hardlink converted count (%d) received count (%d)", expected, hardlinksConvertedCount))
	}
	return
}

func ValidateMessageOutput(a Asserter, stdout AzCopyStdout, message string, shouldContain bool) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}
	var contains bool
	for _, line := range stdout.RawStdout() {
		if strings.Contains(line, message) {
			contains = true
			break
		}
	}
	if (!contains && !shouldContain) || (contains && shouldContain) {
		return
	}
	fmt.Println(stdout.String())
	a.Error(fmt.Sprintf("expected message (%s) not found in azcopy output", message))
}

func ValidateStatsReturned(a Asserter, stdout AzCopyStdout) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}
	csrStdout, ok := stdout.(*AzCopyParsedCopySyncRemoveStdout)
	a.AssertNow("stdout must be AzCopyParsedCopySyncRemoveStdout", Equal{}, ok, true)
	// Check for any of the stats. It's possible for average iops, server busy percentage, network error percentage to be 0, but average e2e milliseconds should never be 0.
	statsFound := csrStdout.FinalStatus.AverageIOPS != 0 || csrStdout.FinalStatus.AverageE2EMilliseconds != 0 || csrStdout.FinalStatus.ServerBusyPercentage != 0 || csrStdout.FinalStatus.NetworkErrorPercentage != 0
	a.Assert("stats must be returned", Equal{}, statsFound, true)
}

func ValidateContainsError(a Asserter, stdout AzCopyStdout, errorMsg []string) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}
	for _, line := range stdout.RawStdout() {
		if checkMultipleErrors(errorMsg, line) {
			return
		}
	}
	fmt.Println(stdout.String())
	a.Error(fmt.Sprintf("expected error message %v not found in azcopy output", errorMsg))
}

// ValidateDoesNotContainError Validates specific errorMsg is not returned in AzCopy run
func ValidateDoesNotContainError(a Asserter, stdout AzCopyStdout, errorMsg []string) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}
	for _, line := range stdout.RawStdout() {
		if !checkMultipleErrors(errorMsg, line) {
			return
		}
	}
	fmt.Println(stdout.String())
	a.Error(fmt.Sprintf("error message %v not expected was found in azcopy output", errorMsg))
}

func checkMultipleErrors(errorMsg []string, line string) bool {
	for _, e := range errorMsg {
		if strings.Contains(line, e) {
			return true
		}
	}

	return false
}

func ValidateListTextOutput(a Asserter, stdout AzCopyStdout, expectedObjects map[AzCopyOutputKey]cmd.AzCopyListObject, expectedSummary *cmd.AzCopyListSummary) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	for _, line := range stdout.RawStdout() {
		if line != "" && !strings.HasPrefix(line, "WARN") {
			// checking summary lines first if they exist
			if strings.Contains(line, "File count:") {
				fileCount := strings.Split(line, ":")
				if strings.TrimSpace(fileCount[1]) != expectedSummary.FileCount {
					a.Error(fmt.Sprintf("File count does not match - raw:%s. expected:%s.", fileCount[1], expectedSummary.FileCount))
				}
			} else if strings.Contains(line, "Total file size:") {
				totalFileSize := strings.Split(line, ":")
				if strings.TrimSpace(totalFileSize[1]) != expectedSummary.TotalFileSize {
					a.Error(fmt.Sprintf("Total file size does not match - raw:%s. expected:%s.", totalFileSize[1], expectedSummary.TotalFileSize))
				}
			} else {
				// convert line into list object
				lo := parseAzCopyListObject(a, line)
				key := AzCopyOutputKey{
					Path:      lo.Path,
					VersionId: lo.VersionId,
				}

				// check if the object exists in map
				expectedLo, ok := expectedObjects[key]
				if !ok {
					a.Error(fmt.Sprintf("%s does not exist in expected objects", key.Path))
				}

				// verify contents of the list object and make sure it matches with the expected object
				eq := reflect.DeepEqual(lo, expectedLo)
				if !eq {
					a.Error(fmt.Sprintf("%#v does not match the expected object %#v.", lo, expectedLo))
				}

				// delete object from expected object after verifying list object exists and is correct
				delete(expectedObjects, key)
			}
		}
	}

	// check if any expected objects were missed
	if len(expectedObjects) != 0 {
		a.Error(fmt.Sprintf("expected objects are not present in the list output %#v", expectedObjects))
	}
}

func parseAzCopyListObject(a Asserter, line string) cmd.AzCopyListObject {
	stdoutParts := strings.Split(line, ";")
	properties := make(map[string]string)

	for i, part := range stdoutParts {
		if i == 0 {
			properties["Path"] = part
		} else {
			val := strings.SplitN(part, ":", 2)
			properties[strings.TrimSpace(val[0])] = strings.TrimSpace(val[1])
		}
	}

	// do some error checking/verification that the elements that are nil don't break this
	var lmt *time.Time
	if properties[string(cmd.LastModifiedTime)] != "" {
		lmtVal, err := time.Parse(cmd.LastModifiedTimeFormat, properties[string(cmd.LastModifiedTime)])
		if err != nil {
			a.Error("error parsing time from lmt string: " + err.Error())
		}
		lmt = &lmtVal
	}

	contentMD5 := []byte(nil)
	md5 := properties[string(cmd.ContentMD5)]
	if md5 != "" {
		decodedContentMD5, err := base64.StdEncoding.DecodeString(md5)
		if err != nil {
			a.Error("error decoding content md5 string: " + err.Error())
		}
		contentMD5 = decodedContentMD5
	}

	return cmd.AzCopyListObject{
		Path:             properties["Path"],
		LastModifiedTime: lmt,
		VersionId:        properties[string(cmd.VersionId)],
		BlobType:         blob.BlobType(properties[string(cmd.BlobType)]),
		BlobAccessTier:   blob.AccessTier(properties[string(cmd.BlobAccessTier)]),
		ContentType:      properties[string(cmd.ContentType)],
		ContentEncoding:  properties[string(cmd.ContentEncoding)],
		ContentMD5:       contentMD5,
		LeaseState:       lease.StateType(properties[string(cmd.LeaseState)]),
		LeaseStatus:      lease.StatusType(properties[string(cmd.LeaseStatus)]),
		LeaseDuration:    lease.DurationType(properties[string(cmd.LeaseDuration)]),
		ArchiveStatus:    blob.ArchiveStatus(properties[string(cmd.ArchiveStatus)]),
		ContentLength:    properties["Content Length"],
	}
}

type DryrunOp uint8

const (
	DryrunOpCopy DryrunOp = iota + 1
	DryrunOpDelete
	DryrunOpProperties
)

var dryrunOpStr = map[DryrunOp]string{
	DryrunOpCopy:       "copy",
	DryrunOpDelete:     "delete",
	DryrunOpProperties: "set-properties",
}

// ValidateDryRunOutput validates output for items in the expected map; expected must equal output
func ValidateDryRunOutput(a Asserter, output AzCopyStdout, rootSrc ResourceManager, rootDst ResourceManager, expected map[string]DryrunOp) {
	if dryrun, ok := a.(DryrunAsserter); ok && dryrun.Dryrun() {
		return
	}
	a.AssertNow("Output must not be nil", Not{IsNil{}}, output)
	stdout, ok := output.(*AzCopyParsedDryrunStdout)
	a.AssertNow("Output must be dryrun stdout", Equal{}, ok, true)

	uriPrefs := GetURIOptions{
		LocalOpts: LocalURIOpts{
			PreferUNCPath: true,
		},
	}

	srcBase := rootSrc.URI(uriPrefs)
	var dstBase string
	if rootDst != nil {
		dstBase = rootDst.URI(uriPrefs)
	}

	if stdout.JsonMode {
		// validation must have nothing in it, and nothing should miss in output.
		validation := CloneMap(expected)

		for _, v := range stdout.Transfers {
			// Determine the op.
			op := common.Iff(v.FromTo.IsDelete(), DryrunOpDelete, common.Iff(v.FromTo.IsSetProperties(), DryrunOpProperties, DryrunOpCopy))

			// Try to find the item in expected.
			relPath := strings.TrimPrefix( // Ensure we start with the rel path, not a separator
				strings.ReplaceAll( // Isolate path separators
					strings.TrimPrefix(v.Source, srcBase), // Isolate the relpath
					"\\", "/",
				),
				"/",
			)
			//a.Log("base %s source %s rel %s", srcBase, v.Source, relPath)
			expectedOp, ok := validation[relPath]
			a.Assert(fmt.Sprintf("Expected %s in map", relPath), Equal{}, ok, true)
			a.Assert(fmt.Sprintf("Expected %s to match", relPath), Equal{}, op, expectedOp)
			if rootDst != nil {
				a.Assert(fmt.Sprintf("Expected %s dest url to match expected dest url", relPath), Equal{}, v.Destination, common.GenerateFullPath(dstBase, relPath))
			}
		}
	} else {
		// It is useless to try to parse details from a user friendly statement.
		// Instead, we should attempt to generate the user friendly statement, and validate that it existed from there.
		validation := make(map[string]bool)

		for k, v := range expected {
			from := common.GenerateFullPath(srcBase, k)
			var to string
			if rootDst != nil {
				to = " to " + common.GenerateFullPath(dstBase, k)
			}

			valStr := fmt.Sprintf("DRYRUN: %s %s%s",
				dryrunOpStr[v],
				from,
				to,
			)

			validation[valStr] = true
		}

		for k := range stdout.Raw {
			_, ok := validation[k]
			a.Assert(k+" wasn't present in validation", Equal{}, ok, true)

			if ok {
				delete(validation, k)
			}
		}

		for k := range validation {
			a.Assert(k+" wasn't present in output", Always{})
		}
	}
}

func ValidateJobsListOutput(a Asserter, stdout AzCopyStdout, expectedJobIDs int, expectedJobs []string) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	jobsListStdout, ok := stdout.(*AzCopyParsedJobsListStdout)
	a.AssertNow("stdout must be AzCopyParsedJobsListStdout", Equal{}, ok, true)
	a.Assert("No of jobs executed should be equivalent", Equal{}, expectedJobIDs, jobsListStdout.JobsCount)

	// Create a set of actual job IDs for efficient lookup
	actualJobs := make(map[string]bool)
	for _, job := range jobsListStdout.Jobs {
		j := job.JobId.String()
		actualJobs[j] = true
	}

	// Check if each expected job is in the actual jobs list
	for _, expectedJob := range expectedJobs {
		if _, found := actualJobs[expectedJob]; !found {
			a.Assert(fmt.Sprintf("expected job with ID %s not found in jobs list", expectedJob), Always{})
		}
	}
}

func ValidateLogFileRetention(a Asserter, logsDir string, expectedLogFileToRetain int) {

	files, err := os.ReadDir(logsDir)
	a.NoError("Failed to read log dir", err)
	cnt := 0

	for _, file := range files { // first, find the job ID
		if strings.HasSuffix(file.Name(), ".log") {
			cnt++
		}
	}
	a.AssertNow("Expected job log files to be retained", Equal{}, cnt, expectedLogFileToRetain)
}
