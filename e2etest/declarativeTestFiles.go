// Copyright © Microsoft <wastore@microsoft.com>
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

package e2etest

import (
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	bfsfile "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

///////////////

type contentHeaders struct {
	// TODO: do we really need/want the fields here to be individually nillable? For Blob, at least, setting these is an all-or-nothing proposition anyway, so maybe there's little need for individual nillablity
	cacheControl       *string
	contentDisposition *string
	contentEncoding    *string
	contentLanguage    *string
	contentType        *string
	contentMD5         []byte
}

func (h *contentHeaders) DeepCopy() *contentHeaders {
	if h == nil {
		return nil
	}
	ret := contentHeaders{}
	ret.cacheControl = h.cacheControl
	ret.contentDisposition = h.contentDisposition
	ret.contentEncoding = h.contentEncoding
	ret.contentLanguage = h.contentLanguage
	ret.contentType = h.contentType
	if h.contentMD5 != nil {
		ret.contentMD5 = make([]byte, len(h.contentMD5))
		copy(ret.contentMD5, h.contentMD5)
	}

	return &ret
}

func (h *contentHeaders) ToBlob() *blob.HTTPHeaders {
	if h == nil {
		return nil
	}

	return &blob.HTTPHeaders{
		BlobContentType:        h.contentType,
		BlobContentDisposition: h.contentDisposition,
		BlobContentEncoding:    h.contentEncoding,
		BlobContentLanguage:    h.contentLanguage,
		BlobCacheControl:       h.cacheControl,
		BlobContentMD5:         h.contentMD5,
	}
}

func (h *contentHeaders) ToFile() *file.HTTPHeaders {
	if h == nil {
		return nil
	}

	return &file.HTTPHeaders{
		ContentType:        h.contentType,
		ContentDisposition: h.contentDisposition,
		ContentEncoding:    h.contentEncoding,
		ContentLanguage:    h.contentLanguage,
		CacheControl:       h.cacheControl,
		ContentMD5:         h.contentMD5,
	}
}

func (h *contentHeaders) ToBlobFS() *bfsfile.HTTPHeaders {
	if h == nil {
		return nil
	}

	return &bfsfile.HTTPHeaders{
		ContentMD5:         h.contentMD5,
		ContentType:        h.contentType,
		ContentDisposition: h.contentDisposition,
		ContentEncoding:    h.contentEncoding,
		ContentLanguage:    h.contentLanguage,
		CacheControl:       h.cacheControl,
	}
}

func (h *contentHeaders) ToCommonHeaders() common.ResourceHTTPHeaders {
	if h == nil {
		return common.ResourceHTTPHeaders{}
	}

	return common.ResourceHTTPHeaders{
		ContentType:        DerefOrZero(h.contentType),
		ContentMD5:         h.contentMD5,
		ContentEncoding:    DerefOrZero(h.contentEncoding),
		ContentLanguage:    DerefOrZero(h.contentLanguage),
		ContentDisposition: DerefOrZero(h.contentDisposition),
		CacheControl:       DerefOrZero(h.cacheControl),
	}
}

func (h *contentHeaders) String() string {
	var ret string
	if h == nil {
		return "[nil]"
	}

	ret += "[\n"

	ret += fmt.Sprintln("cacheControl: " + reflect.ValueOf(h.cacheControl).Elem().String())
	ret += fmt.Sprintln("contentDisposition: " + reflect.ValueOf(h.contentDisposition).Elem().String())
	ret += fmt.Sprintln("contentEncoding: " + reflect.ValueOf(h.contentLanguage).Elem().String())
	ret += fmt.Sprintln("contentType: " + reflect.ValueOf(h.contentType).Elem().String())
	ret += fmt.Sprintln("contentMD5: " + hex.EncodeToString(h.contentMD5))

	ret += "]\n"

	return ret
}

// The full set of properties, dates, info etc, that we can potentially preserve for a file or folder
// This is exposed to the declarativeResourceManagers, to create/check the objects.
// All field are pointers or interfaces to make them nil-able. Nil means "unspecified".
type objectProperties struct {
	entityType         common.EntityType
	symlinkTarget      *string
	posixProperties    *objectUnixStatContainer
	size               *int64
	contentHeaders     *contentHeaders
	nameValueMetadata  map[string]*string
	blobTags           common.BlobTags
	blobType           common.BlobType
	blobVersions       *uint
	creationTime       *time.Time
	lastWriteTime      *time.Time
	smbAttributes      *uint32
	smbPermissionsSddl *string
	adlsPermissionsACL *string // TODO: Test owner and group; needs a good target though.
	cpkInfo            *blob.CPKInfo
	cpkScopeInfo       *blob.CPKScopeInfo
}

type objectUnixStatContainer struct {
	// mode can contain THE FOLLOWING file type specifier bits (common.S_IFSOCK, common.S_IFIFO)
	// common.S_IFDIR and common.S_IFLNK are achievable using folder() and symlink().
	// TODO/Spike: common.S_IFBLK and common.S_IFCHR may be difficult to replicate consistently in a test environment
	mode *uint32

	accessTime *time.Time
	modTime    *time.Time
}

func (o *objectUnixStatContainer) HasTimes() bool {
	return o != nil && (o.accessTime != nil || o.modTime != nil)
}

func (o *objectUnixStatContainer) Empty() bool {
	if o == nil {
		return true
	}

	return o.mode == nil &&
		o.accessTime == nil &&
		o.modTime == nil
}

func (o *objectUnixStatContainer) DeepCopy() *objectUnixStatContainer {
	if o == nil {
		return nil
	}
	out := &objectUnixStatContainer{}

	if o.mode != nil {
		mode := *o.mode
		out.mode = &mode
	}

	if o.accessTime != nil {
		accessTime := *o.accessTime
		out.accessTime = &accessTime
	}

	if o.modTime != nil {
		modTime := *o.modTime
		out.modTime = &modTime
	}

	return out
}

func (o *objectUnixStatContainer) EquivalentToStatAdapter(s common.UnixStatAdapter) string {
	if o == nil {
		return "" // no comparison to make
	}

	mismatched := make([]string, 0)
	// only compare if we set it
	if o.mode != nil {
		if s.FileMode() != *o.mode {
			mismatched = append(mismatched, "mode")
		}
	}

	if o.accessTime != nil {
		if o.accessTime.UnixNano() != s.ATime().UnixNano() {
			mismatched = append(mismatched, "atime")
		}
	}

	if o.modTime != nil {
		if o.modTime.UnixNano() != s.MTime().UnixNano() {
			mismatched = append(mismatched, "mtime")
		}
	}

	return strings.Join(mismatched, ", ")
}

func (o *objectUnixStatContainer) AddToMetadata(metadata map[string]*string, style common.PosixPropertiesStyle) {
	if o == nil {
		return
	}

	mask := uint32(0)

	if o.mode != nil { // always overwrite; perhaps it got changed in one of the hooks.
		mask |= common.STATX_MODE
		// Use style to determine format to store
		if style == common.AMLFSPosixPropertiesStyle {
			metadata[common.POSIXModeMeta] = to.Ptr(fmt.Sprintf("%04o", uint64(*o.mode&0777)))
		} else {
			metadata[common.POSIXModeMeta] = to.Ptr(strconv.FormatUint(uint64(*o.mode), 10))
		}

		delete(metadata, common.POSIXFIFOMeta)
		delete(metadata, common.POSIXSocketMeta)
		switch {
		case *o.mode&common.S_IFIFO == common.S_IFIFO:
			metadata[common.POSIXFIFOMeta] = to.Ptr("true")
		case *o.mode&common.S_IFSOCK == common.S_IFSOCK:
			metadata[common.POSIXSocketMeta] = to.Ptr("true")
		}
	}

	if o.accessTime != nil {
		mask |= common.STATX_ATIME
		metadata[common.POSIXATimeMeta] = to.Ptr(strconv.FormatInt(o.accessTime.UnixNano(), 10))
	}

	if o.modTime != nil {
		mask |= common.STATX_MTIME
		// Use style to determine format to store
		if style == common.AMLFSPosixPropertiesStyle {
			metadata[common.POSIXModTimeMeta] = to.Ptr(o.modTime.Format(common.AMLFS_MOD_TIME_LAYOUT))
		} else {
			metadata[common.POSIXModTimeMeta] = to.Ptr(strconv.FormatInt(o.modTime.UnixNano(), 10))
		}
	}

	metadata[common.LINUXStatxMaskMeta] = to.Ptr(strconv.FormatUint(uint64(mask), 10))
}

// returns op.size, if present, else defaultSize
func (op objectProperties) sizeBytes(a asserter, defaultSize string) int {
	if op.size != nil {
		if *op.size > math.MaxInt32 {
			a.Error(fmt.Sprintf("unsupported size: %d", *op.size))
			return 0
		}
		return int(*op.size)
	}

	longSize, err := cmd.ParseSizeString(defaultSize, "testFiles.size")
	if longSize < math.MaxInt32 {
		a.AssertNoErr(err)
		return int(longSize)
	}
	a.Error("unsupported size: " + defaultSize)
	return 0
}

func (op objectProperties) DeepCopy() objectProperties {
	ret := objectProperties{}
	ret.entityType = op.entityType

	if op.symlinkTarget != nil {
		target := *op.symlinkTarget
		ret.symlinkTarget = &target
	}

	if !op.posixProperties.Empty() {
		ret.posixProperties = op.posixProperties.DeepCopy()
	}

	if op.size != nil {
		val := op.size
		ret.size = val
	}

	if op.contentHeaders != nil {
		ret.contentHeaders = op.contentHeaders.DeepCopy()
	}

	ret.nameValueMetadata = make(map[string]*string)
	for k, v := range op.nameValueMetadata {
		ret.nameValueMetadata[k] = v
	}

	ret.blobTags = make(map[string]string)
	for k, v := range op.blobTags {
		ret.blobTags[k] = v
	}

	if op.blobVersions != nil {
		ret.blobVersions = pointerTo(*op.blobVersions)
	}

	if op.creationTime != nil {
		time := *op.creationTime
		ret.creationTime = &time
	}

	if op.lastWriteTime != nil {
		time := *op.lastWriteTime
		ret.lastWriteTime = &time
	}

	if op.smbAttributes != nil {
		val := *op.smbAttributes
		ret.smbAttributes = &val
	}

	if op.smbPermissionsSddl != nil {
		val := *op.smbPermissionsSddl
		ret.smbPermissionsSddl = &val
	}

	if op.adlsPermissionsACL != nil {
		val := *op.adlsPermissionsACL
		ret.adlsPermissionsACL = &val
	}

	if op.cpkInfo != nil {
		val := *op.cpkInfo
		ret.cpkInfo = &val
	}

	if op.cpkScopeInfo != nil {
		val := *op.cpkScopeInfo
		ret.cpkScopeInfo = &val
	}

	return ret
}

// a file or folder. Create these with the f() and folder() functions
type testObject struct {
	name                   string
	expectedFailureMessage string // the failure message that we expect to see in the log for this file/folder (only populated for expected failures)

	body []byte

	// info to be used at creation time. Usually, creationInfo and verificationInfo will be the same
	// I.e. we expect the creation properties to be preserved. But, for flexibility, they can be set to something different.
	creationProperties objectProperties
	// info to be used at verification time. Will be nil if there is no validation (of properties) to be done
	verificationProperties *objectProperties
}

func (t *testObject) DeepCopy() *testObject {
	ret := testObject{}
	ret.name = t.name
	ret.expectedFailureMessage = t.expectedFailureMessage
	ret.creationProperties = t.creationProperties.DeepCopy()

	if t.body != nil {
		ret.body = make([]byte, len(t.body))
		copy(ret.body, t.body)
	}

	if t.verificationProperties != nil {
		vp := (*t.verificationProperties).DeepCopy()
		ret.verificationProperties = &vp
	}

	return &ret
}

func (t *testObject) hasContentToValidate() bool {
	if t.verificationProperties != nil && t.creationProperties.entityType != t.verificationProperties.entityType {
		panic("entityType property is misconfigured")
	}

	return t.creationProperties.entityType == common.EEntityType.File()
}

func (t *testObject) isFolder() bool {
	if t.verificationProperties != nil && t.creationProperties.entityType != t.verificationProperties.entityType {
		panic("entityType property is misconfigured")
	}

	return t.creationProperties.entityType == common.EEntityType.Folder()
}

func (t *testObject) isRootFolder() bool {
	return t.name == "" && t.isFolder()
}

// This interface is implemented by types that provide extra information about test files
// It is to be used ONLY as parameters to the f() and folder() methods.
// It is not used in other parts of the code, since the other parts use the testObject instances that are created
// from
type withPropertyProvider interface {
	appliesToCreation() bool
	appliesToVerification() bool

	createObjectProperties() *objectProperties
}

type expectedFailureProvider interface {
	expectedFailure() string
}

// Define a file, in the expectations lists on a testFiles struct.
// (Note, if you are not going to supply any parameters other than the name, you can just use a string literal in the list
// instead of calling this function).
// Provide properties by including one or more objects that implement withPropertyProvider.
// Typically, that will just be done like this: f("foo", with{<set properties here>})
// For advanced cases, you can use verifyOnly instead of the normal "with".  The normal "with" applies to both creation
// and verification.
// You can also add withFailureMessage{"message"} to files that are expected to fail, to specify what the expected
// failure message will be in the log.
// And withStubMetadata{} to supply the metadata that indicates that an object is a directory stub.
func f(n string, properties ...withPropertyProvider) *testObject {
	haveCreationProperties := false
	haveVerificationProperties := false

	result := &testObject{name: n}
	for _, p := range properties {

		// special case for specifying expected failure message
		if efp, ok := p.(expectedFailureProvider); ok {
			if p.createObjectProperties() != nil {
				panic("a withPropertyProvider that implements expectedFailureProvider should not provide any objectProperties. It ONLY specifies the failure message.")
			}
			result.expectedFailureMessage = efp.expectedFailure()
			continue
		}

		// normal case. Not that normally the same p will return true for both p.appliesToCreation and p.appliesToVerification
		const mustBeOne = "there must be only one withPropertyProvider that specifies the %s properties.  You can't mix 'with{...}' and 'with%sOnly{...}. But you can mix 'withCreationOnly{...}' and 'withVerificationOnly{...}"
		if p.appliesToCreation() {
			if haveCreationProperties {
				panic(fmt.Sprintf(mustBeOne, "creation", "Creation"))
			}
			haveCreationProperties = true
			objProps := p.createObjectProperties()
			if objProps == nil {
				objProps = &objectProperties{} // for creationProperties, this saves our code from endless nil checks. (But for verification, below, the nil is useful)
			}
			result.creationProperties = *objProps
		}

		if p.appliesToVerification() {
			if haveVerificationProperties {
				panic(fmt.Sprintf(mustBeOne, "verification", "Verification"))
			}
			haveVerificationProperties = true
			objProps := p.createObjectProperties()
			result.verificationProperties = objProps // verification props is nilable, and nil signals "nothing to verify"
		}
	}

	return result
}

func symlink(new, target string) *testObject {
	name := strings.TrimLeft(new, "/")
	result := f(name)

	// result.creationProperties
	result.creationProperties.entityType = common.EEntityType.Symlink()
	result.creationProperties.symlinkTarget = &target

	result.verificationProperties = &objectProperties{}
	result.verificationProperties.entityType = common.EEntityType.Symlink()
	result.verificationProperties.symlinkTarget = &target

	return result
}

// define a folder, in the expectations lists on a testFiles struct
func folder(n string, properties ...withPropertyProvider) *testObject {
	name := strings.TrimLeft(n, "/")
	result := f(name, properties...)

	// isFolder is at properties level, not testObject level, because we need it at properties level when reading
	// the properties back from the destination (where we don't read testObjects, we just read objectProperties)
	result.creationProperties.entityType = common.EEntityType.Folder()
	if result.verificationProperties != nil {
		result.verificationProperties.entityType = common.EEntityType.Folder()
	}

	return result
}

//////////

type objectTarget struct {
	objectName string
	snapshotid bool // add snapshot id
	// versions specifies a zero-indexed list of versions to copy.
	// ID is automatically filled in based off the versions specified in this field.
	// Nil or empty list does nothing. A single version ID will be passed as a part of the URI,
	// unless singleVersionList is true.
	// Negative cases for list of versions, e.g. specifying nonexistent versions, shouldn't be done here.
	// Those get trimmed out by the traverser.
	versions          []uint
	singleVersionList bool
}

// Represents a set of source files, including what we expect should happen to them
// Our expectations, e.g. success or failure, are represented by whether we put each file into
// "shouldTransfer", "shouldFail" etc.
type testFiles struct {
	defaultSize  string                      // how big should the files be? Applies to those files that don't specify individual sizes. Uses the same K, M, G suffixes as benchmark mode's size-per-file
	objectTarget objectTarget                // should we target only a single file/folder?
	destTarget   string                      // do we want to copy under a folder or rename?
	sourcePublic *container.PublicAccessType // should the source blob container be public? (ONLY APPLIES TO BLOB.)

	// The files/folders that we expect to be transferred. Elements of the list must be strings or testObject's.
	// A string can be used if no properties need to be specified.
	// Folders included here are ignored by the verification code when we are not transferring between folder-aware
	// locations.
	shouldTransfer []interface{}

	// the files/folders that we expect NOT to be found by the enumeration. See comments on shouldTransfer
	shouldIgnore []interface{}

	// the files/folders that we expect to  fail with error (unlike the other fields, this one is composite object instead of just a filename
	shouldFail []interface{}

	// files/folders that we expect to be skipped due to an overwrite setting
	shouldSkip []interface{}
}

func (tf testFiles) cloneShouldTransfers() testFiles {
	return testFiles{
		defaultSize:    tf.defaultSize,
		objectTarget:   tf.objectTarget,
		destTarget:     tf.destTarget,
		sourcePublic:   tf.sourcePublic,
		shouldTransfer: tf.shouldTransfer,
	}
}

func (tf testFiles) DeepCopy() testFiles {
	ret := testFiles{}
	ret.defaultSize = tf.defaultSize

	ret.objectTarget = tf.objectTarget
	ret.destTarget = tf.destTarget
	ret.sourcePublic = tf.sourcePublic
	ret.shouldTransfer = tf.copyList(tf.shouldTransfer)
	ret.shouldIgnore = tf.copyList(tf.shouldIgnore)
	ret.shouldFail = tf.copyList(tf.shouldFail)
	ret.shouldSkip = tf.copyList(tf.shouldSkip)
	return ret
}

func (*testFiles) copyList(src []interface{}) []interface{} {
	var ret []interface{}
	for _, r := range src {
		if aTestObj, ok := r.(*testObject); ok {
			ret = append(ret, aTestObj.DeepCopy())
		} else if asString, ok := r.(string); ok {
			ret = append(ret, asString)
		} else {
			panic("testFiles lists may contain only strings and testObjects. Create your test objects with the f() and folder() functions")
		}
	}
	return ret
}

// takes a mixed list of (potentially) strings and testObjects, and returns them all as test objects
// TODO: do we want to continue supporting plain strings in the expectation file lists (for convenience of test coders)
//
//	or force them to use f() for every file?
func (*testFiles) toTestObjects(rawList []interface{}, isFail bool) []*testObject {
	result := make([]*testObject, 0, len(rawList))
	for k, r := range rawList {
		if asTestObject, ok := r.(*testObject); ok {
			if asTestObject.expectedFailureMessage != "" && !isFail {
				panic("expected failures are only allowed in the shouldFail list. They are not allowed for other test files")
			}
			result = append(result, asTestObject)
		} else if asString, ok := r.(string); ok {
			rawList[k] = &testObject{name: asString} // convert to a full deal so we can apply md5
			result = append(result, rawList[k].(*testObject))
		} else {
			panic("testFiles lists may contain only strings and testObjects. Create your test objects with the f() and folder() functions")
		}
	}
	return result
}

func (tf *testFiles) allObjects(isSource bool) []*testObject {
	if isSource {
		result := make([]*testObject, 0)
		result = append(result, tf.toTestObjects(tf.shouldTransfer, false)...)
		result = append(result, tf.toTestObjects(tf.shouldIgnore, false)...) // these must be present at the source. Enumeration filters are expected to skip them
		result = append(result, tf.toTestObjects(tf.shouldSkip, false)...)   // these must be present at the source. Overwrite processing is expected to skip them
		result = append(result, tf.toTestObjects(tf.shouldFail, true)...)    // these must also be present at the source. Their transferring is expected to fail
		return result
	}
	// destination only needs the things that overwrite will skip
	return tf.toTestObjects(tf.shouldSkip, false)
}

func (tf *testFiles) isListOfVersions() bool {
	return tf.objectTarget.objectName != "" && (len(tf.objectTarget.versions) > 1 || (len(tf.objectTarget.versions) == 1 && tf.objectTarget.singleVersionList))
}

func (tf *testFiles) getForStatus(s *scenario, status common.TransferStatus, expectFolders bool, expectRootFolder bool) []*testObject {
	if status == common.ETransferStatus.Success() && tf.isListOfVersions() {
		s.a.Assert(s.fromTo.From(), equals(), common.ELocation.Blob(), "List of Versions must be used with Blob")
		versions := s.GetSource().(*resourceBlobContainer).getVersions(s.a, tf.objectTarget.objectName)

		// track down the original testObject
		var target *testObject
		for _, v := range tf.toTestObjects(tf.shouldTransfer, false) {
			if v.name == tf.objectTarget.objectName {
				target = v
				break
			}
		}
		s.a.Assert(target, notEquals(), nil, "objectTarget must exist in successful transfers")

		out := make([]*testObject, len(tf.objectTarget.versions))
		for k, v := range tf.objectTarget.versions {
			// flatten the version ID
			versions[v] = strings.ReplaceAll(versions[v], ":", "-")
			out[k] = target.DeepCopy()
			out[k].name = versions[v] + "-" + out[k].name
		}
		return out
	}

	shouldInclude := func(f *testObject) bool {
		if !f.isFolder() {
			return true
		}

		if expectFolders {
			if f.isRootFolder() {
				return expectRootFolder
			}
			return true
		}
		return false
	}

	result := make([]*testObject, 0)
	switch status {
	case common.ETransferStatus.Success():
		for _, f := range tf.toTestObjects(tf.shouldTransfer, false) {
			if shouldInclude(f) {
				result = append(result, f)
			}
		}
	case common.ETransferStatus.Failed():
		for _, f := range tf.toTestObjects(tf.shouldFail, true) {
			if shouldInclude(f) {
				result = append(result, f)
			}
		}
	default:
		panic("unsupported status type")
	}
	return result
}
