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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	datalakefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	sharefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/JeffreyRichter/enum/enum"
)

const (
	AZCOPY_PATH_SEPARATOR_STRING = "/"
	OS_PATH_SEPARATOR            = string(os.PathSeparator)
	EXTENDED_PATH_PREFIX         = `\\?\`
	EXTENDED_UNC_PATH_PREFIX     = `\\?\UNC`
	Dev_Null                     = os.DevNull

	//  this is the perm that AzCopy has used throughout its preview.  So, while we considered relaxing it to 0666
	//  we decided that the best option was to leave it as is, and only relax it if user feedback so requires.
	DEFAULT_FILE_PERM = 0644 // the os package will handle base-10 for us.

	// Since we haven't updated the Go SDKs to handle CPK just yet, we need to detect CPK related errors
	// and inform the user that we don't support CPK yet.
	CPK_ERROR_SERVICE_CODE    = "BlobUsesCustomerSpecifiedEncryption"
	FILE_NOT_FOUND            = "The specified file was not found."
	EINTR_RETRY_COUNT         = 5
	RECOMMENDED_OBJECTS_COUNT = 10000000
	WARN_MULTIPLE_PROCESSES   = "More than one AzCopy process is running. This is a non-blocking warning, AzCopy will continue the operation. \n But, it is best practice to run a single process per VM." +
		"\nPlease terminate other instances." // This particular warning message does not abort the whole operation
	AMLFS_MOD_TIME_LAYOUT = "2006-01-02 15:04:05 -0700"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// this struct is used to parse the contents of file passed with list-of-files flag.
type ListOfFiles struct {
	Files []string
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type JobID UUID

func NewJobID() JobID {
	return JobID(NewUUID())
}

// var EmptyJobId JobID = JobID{}
func (j JobID) IsEmpty() bool {
	return j == JobID{}
}

func ParseJobID(jobID string) (JobID, error) {
	uuid, err := ParseUUID(jobID)
	if err != nil {
		return JobID{}, err
	}
	return JobID(uuid), nil
}

func (j JobID) String() string {
	return UUID(j).String()
}

// Implementing MarshalJSON() method for type JobID
func (j JobID) MarshalJSON() ([]byte, error) {
	return json.Marshal(UUID(j))
}

// Implementing UnmarshalJSON() method for type JobID
func (j *JobID) UnmarshalJSON(b []byte) error {
	var u UUID
	if err := json.Unmarshal(b, &u); err != nil {
		return err
	}
	*j = JobID(u)
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type PartNumber uint32
type Version uint32
type Status uint32

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var EDeleteSnapshotsOption = DeleteSnapshotsOption(0)

type DeleteSnapshotsOption uint8

func (DeleteSnapshotsOption) None() DeleteSnapshotsOption    { return DeleteSnapshotsOption(0) }
func (DeleteSnapshotsOption) Include() DeleteSnapshotsOption { return DeleteSnapshotsOption(1) }
func (DeleteSnapshotsOption) Only() DeleteSnapshotsOption    { return DeleteSnapshotsOption(2) }

func (d DeleteSnapshotsOption) String() string {
	return enum.StringInt(d, reflect.TypeOf(d))
}

func (d *DeleteSnapshotsOption) Parse(s string) error {
	// allow empty to mean "None"
	if s == "" {
		*d = EDeleteSnapshotsOption.None()
		return nil
	}

	val, err := enum.ParseInt(reflect.TypeOf(d), s, true, true)
	if err == nil {
		*d = val.(DeleteSnapshotsOption)
	}
	return err
}

func (d DeleteSnapshotsOption) ToDeleteSnapshotsOptionType() *blob.DeleteSnapshotsOptionType {
	if d == EDeleteSnapshotsOption.None() {
		return nil
	}

	return to.Ptr(blob.DeleteSnapshotsOptionType(strings.ToLower(d.String())))
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var ETrailingDotOption = TrailingDotOption(0)

type TrailingDotOption uint8

func (TrailingDotOption) Enable() TrailingDotOption                   { return TrailingDotOption(0) }
func (TrailingDotOption) Disable() TrailingDotOption                  { return TrailingDotOption(1) }
func (TrailingDotOption) AllowToUnsafeDestination() TrailingDotOption { return TrailingDotOption(2) }

// Trailing dots are supported in the Enable and AllowToUnsafeDestination options
func (d TrailingDotOption) IsEnabled() bool {
	return d == d.Enable() ||
		d == d.AllowToUnsafeDestination()
}

func (d TrailingDotOption) String() string {
	return enum.StringInt(d, reflect.TypeOf(d))
}

func (d *TrailingDotOption) Parse(s string) error {
	// allow empty to mean "Enable"
	if s == "" {
		*d = ETrailingDotOption.Enable()
		return nil
	}

	val, err := enum.ParseInt(reflect.TypeOf(d), s, true, true)
	if err == nil {
		*d = val.(TrailingDotOption)
	}
	return err
}

func ValidTrailingDotOptions() []string {
	return []string{
		ETrailingDotOption.Enable().String(),
		ETrailingDotOption.Disable().String(),
		ETrailingDotOption.AllowToUnsafeDestination().String(),
	}
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var EBlobTraverserIncludeOption eBlobTraverserIncludeOption

type eBlobTraverserIncludeOption bool

type BlobTraverserIncludeOption uint8

func (eBlobTraverserIncludeOption) Snapshots() BlobTraverserIncludeOption { return 1 }
func (eBlobTraverserIncludeOption) Versions() BlobTraverserIncludeOption  { return 1 << 1 }
func (eBlobTraverserIncludeOption) Deleted() BlobTraverserIncludeOption   { return 1 << 2 }
func (eBlobTraverserIncludeOption) DirStubs() BlobTraverserIncludeOption  { return 1 << 3 } // whether to include blobs that have metadata 'hdi_isfolder = true'
func (eBlobTraverserIncludeOption) None() BlobTraverserIncludeOption      { return 0 }

func (e eBlobTraverserIncludeOption) FromInputs(pdo PermanentDeleteOption, listVersions, includeDirectoryStubs bool) BlobTraverserIncludeOption {
	out := e.None()

	if includeDirectoryStubs {
		out = out.Add(e.DirStubs())
	}

	if pdo != 0 {
		out = out.Add(e.Deleted())

		if pdo.Includes(EPermanentDeleteOption.Snapshots()) {
			out = out.Add(e.Snapshots())
		}

		if pdo.Includes(EPermanentDeleteOption.Versions()) || listVersions {
			out = out.Add(e.Versions())
		}

		return out
	}

	if listVersions {
		out = out.Add(e.Versions())
	}

	return out
}

func (o BlobTraverserIncludeOption) Add(other BlobTraverserIncludeOption) BlobTraverserIncludeOption {
	return o | other
}
func (o BlobTraverserIncludeOption) Includes(other BlobTraverserIncludeOption) bool {
	return (o & other) == other
}

func (o BlobTraverserIncludeOption) Snapshots() bool {
	return o.Includes(EBlobTraverserIncludeOption.Snapshots())
}
func (o BlobTraverserIncludeOption) Versions() bool {
	return o.Includes(EBlobTraverserIncludeOption.Versions())
}
func (o BlobTraverserIncludeOption) Deleted() bool {
	return o.Includes(EBlobTraverserIncludeOption.Deleted())
}
func (o BlobTraverserIncludeOption) DirStubs() bool {
	return o.Includes(EBlobTraverserIncludeOption.DirStubs())
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var EPermanentDeleteOption = PermanentDeleteOption(0) // Default to "None"

type PermanentDeleteOption uint8

func (PermanentDeleteOption) Snapshots() PermanentDeleteOption { return PermanentDeleteOption(1) }
func (PermanentDeleteOption) Versions() PermanentDeleteOption  { return PermanentDeleteOption(1 << 1) }
func (p PermanentDeleteOption) SnapshotsAndVersions() PermanentDeleteOption {
	return p.Snapshots() | p.Versions()
}
func (PermanentDeleteOption) None() PermanentDeleteOption { return PermanentDeleteOption(0) }

func (p PermanentDeleteOption) Includes(other PermanentDeleteOption) bool {
	return (p & other) == other
}

func (p *PermanentDeleteOption) Parse(s string) error {
	// allow empty to mean "None"
	if s == "" {
		*p = EPermanentDeleteOption.None()
		return nil
	}

	val, err := enum.Parse(reflect.TypeOf(p), s, true)
	if err == nil {
		*p = val.(PermanentDeleteOption)
	}
	return err
}

func (p PermanentDeleteOption) String() string {
	return enum.StringInt(p, reflect.TypeOf(p))
}

func (p PermanentDeleteOption) ToPermanentDeleteOptionType() *blob.DeleteType {
	if p == EPermanentDeleteOption.None() {
		return nil
	}
	return to.Ptr(blob.DeleteTypePermanent)
}

func ValidPermanentDeleteOptions() []string {
	return []string{
		EPermanentDeleteOption.None().String(),
		EPermanentDeleteOption.Snapshots().String(),
		EPermanentDeleteOption.Versions().String(),
		EPermanentDeleteOption.SnapshotsAndVersions().String(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type DeleteDestination uint32

var EDeleteDestination = DeleteDestination(0)

func (DeleteDestination) False() DeleteDestination  { return DeleteDestination(0) }
func (DeleteDestination) Prompt() DeleteDestination { return DeleteDestination(1) }
func (DeleteDestination) True() DeleteDestination   { return DeleteDestination(2) }

func (dd *DeleteDestination) Parse(s string) error {
	val, err := enum.Parse(reflect.TypeOf(dd), s, true)
	if err == nil {
		*dd = val.(DeleteDestination)
	}
	return err
}

func (dd DeleteDestination) String() string {
	return enum.StringInt(dd, reflect.TypeOf(dd))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// represents one possible response
var EResponseOption = ResponseOption{ResponseType: "", UserFriendlyResponseType: "", ResponseString: ""}

type ResponseOption struct {
	ResponseType             string // helps us clarify the user's intent and support partner team's localization
	UserFriendlyResponseType string // text to print in interactive mode
	ResponseString           string // short (abbreviation) string that gets sent back by the user to indicate that this response is chosen
}

// NOTE: these enums are shared with StgExp, so the text is spelled out explicitly (for easy json marshalling)
func (ResponseOption) Yes() ResponseOption {
	return ResponseOption{ResponseType: "Yes", UserFriendlyResponseType: "Yes", ResponseString: "y"}
}
func (ResponseOption) No() ResponseOption {
	return ResponseOption{ResponseType: "No", UserFriendlyResponseType: "No", ResponseString: "n"}
}
func (ResponseOption) YesForAll() ResponseOption {
	return ResponseOption{ResponseType: "YesForAll", UserFriendlyResponseType: "Yes for all", ResponseString: "a"}
}
func (ResponseOption) NoForAll() ResponseOption {
	return ResponseOption{ResponseType: "NoForAll", UserFriendlyResponseType: "No for all", ResponseString: "l"}
}
func (ResponseOption) Default() ResponseOption {
	return ResponseOption{ResponseType: "", UserFriendlyResponseType: "", ResponseString: ""}
}

func (o *ResponseOption) Parse(s string) error {
	val, err := enum.Parse(reflect.TypeOf(o), s, true)
	if err == nil {
		*o = val.(ResponseOption)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EOverwriteOption = OverwriteOption(0)

type OverwriteOption uint8

func (OverwriteOption) True() OverwriteOption            { return OverwriteOption(0) }
func (OverwriteOption) False() OverwriteOption           { return OverwriteOption(1) }
func (OverwriteOption) Prompt() OverwriteOption          { return OverwriteOption(2) }
func (OverwriteOption) IfSourceNewer() OverwriteOption   { return OverwriteOption(3) }
func (OverwriteOption) PosixProperties() OverwriteOption { return OverwriteOption(4) }

func (o *OverwriteOption) Parse(s string) error {
	val, err := enum.Parse(reflect.TypeOf(o), s, true)
	if err == nil {
		*o = val.(OverwriteOption)
	}
	return err
}

func (o OverwriteOption) String() string {
	return enum.StringInt(o, reflect.TypeOf(o))
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type LogLevel uint8

const (
	// LogNone tells a logger not to log any entries passed to it.
	LogNone LogLevel = iota

	// LogFatal tells a logger to log all LogFatal entries passed to it.
	LogFatal

	// LogPanic tells a logger to log all LogPanic and LogFatal entries passed to it.
	LogPanic

	// LogError tells a logger to log all LogError, LogPanic and LogFatal entries passed to it.
	LogError

	// LogWarning tells a logger to log all LogWarning, LogError, LogPanic and LogFatal entries passed to it.
	LogWarning

	// LogInfo tells a logger to log all LogInfo, LogWarning, LogError, LogPanic and LogFatal entries passed to it.
	LogInfo

	// LogDebug tells a logger to log all LogDebug, LogInfo, LogWarning, LogError, LogPanic and LogFatal entries passed to it.
	LogDebug
)

var ELogLevel = LogLevel(LogNone)

func (LogLevel) None() LogLevel    { return LogLevel(LogNone) }
func (LogLevel) Fatal() LogLevel   { return LogLevel(LogFatal) }
func (LogLevel) Panic() LogLevel   { return LogLevel(LogPanic) }
func (LogLevel) Error() LogLevel   { return LogLevel(LogError) }
func (LogLevel) Warning() LogLevel { return LogLevel(LogWarning) }
func (LogLevel) Info() LogLevel    { return LogLevel(LogInfo) }
func (LogLevel) Debug() LogLevel   { return LogLevel(LogDebug) }

func (ll *LogLevel) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ll), s, true, true)
	if err == nil {
		*ll = val.(LogLevel)
	}
	return err
}

func (ll LogLevel) String() string {
	switch ll {
	case ELogLevel.None():
		return "NONE"
	case ELogLevel.Fatal():
		return "FATAL"
	case ELogLevel.Panic():
		return "PANIC"
	case ELogLevel.Error():
		return "ERR"
	case ELogLevel.Warning():
		return "WARN"
	case ELogLevel.Info():
		return "INFO"
	case ELogLevel.Debug():
		return "DBG"
	default:
		return enum.StringInt(ll, reflect.TypeOf(ll))
	}
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// LogSanitizer can be implemented to clean secrets from lines logged by ForceLog
// By default no implementation is provided here, because pipeline may be used in many different
// contexts, so the correct implementation is context-dependent
type LogSanitizer interface {
	SanitizeLogMessage(raw string) string
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var EJobPriority = JobPriority(0)

// JobPriority defines the transfer priorities supported by the Storage Transfer Engine's channels
// The default priority is Normal
type JobPriority uint8

func (JobPriority) Normal() JobPriority { return JobPriority(0) }
func (JobPriority) Low() JobPriority    { return JobPriority(1) }
func (jp JobPriority) String() string {
	return enum.StringInt(uint8(jp), reflect.TypeOf(jp))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EJobStatus = JobStatus(0)

// JobStatus indicates the status of a Job; the default is InProgress.
type JobStatus uint32 // Must be 32-bit for atomic operations

func (j *JobStatus) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(j), s, true, true)
	if err == nil {
		*j = val.(JobStatus)
	}
	return err
}

// Implementing MarshalJSON() method for type JobStatus
func (j JobStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(j.String())
}

// Implementing UnmarshalJSON() method for type JobStatus
func (j *JobStatus) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return j.Parse(s)
}

func (j *JobStatus) AtomicLoad() JobStatus {
	return JobStatus(atomic.LoadUint32((*uint32)(j)))
}

func (j *JobStatus) AtomicStore(newJobStatus JobStatus) {
	atomic.StoreUint32((*uint32)(j), uint32(newJobStatus))
}

func (j *JobStatus) EnhanceJobStatusInfo(skippedTransfers, failedTransfers, successfulTransfers bool) JobStatus {
	if failedTransfers && skippedTransfers {
		return EJobStatus.CompletedWithErrorsAndSkipped()
	} else if failedTransfers {
		if successfulTransfers {
			return EJobStatus.CompletedWithErrors()
		} else {
			return EJobStatus.Failed()
		}
	} else if skippedTransfers {
		return EJobStatus.CompletedWithSkipped()
	} else {
		return EJobStatus.Completed()
	}
}

func (j *JobStatus) IsJobDone() bool {
	return *j == EJobStatus.Completed() || *j == EJobStatus.Cancelled() || *j == EJobStatus.CompletedWithSkipped() ||
		*j == EJobStatus.CompletedWithErrors() || *j == EJobStatus.CompletedWithErrorsAndSkipped() ||
		*j == EJobStatus.Failed()
}

func (JobStatus) All() JobStatus                           { return JobStatus(100) }
func (JobStatus) InProgress() JobStatus                    { return JobStatus(0) }
func (JobStatus) Paused() JobStatus                        { return JobStatus(1) }
func (JobStatus) Cancelling() JobStatus                    { return JobStatus(2) }
func (JobStatus) Cancelled() JobStatus                     { return JobStatus(3) }
func (JobStatus) Completed() JobStatus                     { return JobStatus(4) }
func (JobStatus) CompletedWithErrors() JobStatus           { return JobStatus(5) }
func (JobStatus) CompletedWithSkipped() JobStatus          { return JobStatus(6) }
func (JobStatus) CompletedWithErrorsAndSkipped() JobStatus { return JobStatus(7) }
func (JobStatus) Failed() JobStatus                        { return JobStatus(8) }
func (js JobStatus) String() string {
	return enum.StringInt(js, reflect.TypeOf(js))
}

////////////////////////////////////////////////////////////////

var ELocation = Location(0)

// Location indicates the type of Location
type Location uint8

func (Location) Unknown() Location   { return Location(0) }
func (Location) Local() Location     { return Location(1) }
func (Location) Pipe() Location      { return Location(2) }
func (Location) Blob() Location      { return Location(3) }
func (Location) File() Location      { return Location(4) }
func (Location) BlobFS() Location    { return Location(5) }
func (Location) S3() Location        { return Location(6) }
func (Location) Benchmark() Location { return Location(7) }
func (Location) GCP() Location       { return Location(8) }
func (Location) None() Location      { return Location(9) } // None is used in case we're transferring properties
func (Location) FileNFS() Location   { return Location(10) }

func (Location) AzureAccount() Location { return Location(100) } // AzureAccount is never used within AzCopy, and won't be detected, (for now)

func (l Location) String() string {
	return enum.StringInt(l, reflect.TypeOf(l))
}
func (l *Location) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(l), s, true, true)
	if err == nil {
		*l = val.(Location)
	}
	return err
}

// AllStandardLocations returns all locations that are "normal" for testing purposes. Excludes the likes of Unknown, Benchmark and Pipe
func (Location) AllStandardLocations() []Location {
	return []Location{
		ELocation.Local(),
		ELocation.Blob(),
		ELocation.File(),
		ELocation.BlobFS(),
		ELocation.S3(),
		ELocation.FileNFS(),
		// TODO: ELocation.GCP
	}
}

// FromToValue returns the fromTo enum value for given
// from / To location combination. In 16 bits fromTo
// value, first 8 bits represents from location
func FromToValue(from Location, to Location) FromTo {
	return FromTo((FromTo(from) << 8) | FromTo(to))
}

func (l Location) IsRemote() bool {
	switch l {
	case ELocation.BlobFS(), ELocation.Blob(), ELocation.File(), ELocation.S3(), ELocation.GCP(), ELocation.FileNFS():
		return true
	case ELocation.Local(), ELocation.Benchmark(), ELocation.Pipe(), ELocation.Unknown(), ELocation.None():
		return false
	default:
		panic("unexpected location, please specify if it is remote")
	}
}

func (l Location) IsLocal() bool {
	if l == ELocation.Unknown() {
		return false
	} else {
		return !l.IsRemote()
	}
}

// IsAzure checks if location is Azure (BlobFS, Blob, File)
func (l Location) IsAzure() bool {
	return l == ELocation.BlobFS() || l == ELocation.Blob() || l == ELocation.File() || l == ELocation.FileNFS()
}

// IsFolderAware returns true if the location has real folders (e.g. there's such a thing as an empty folder,
// and folders may have properties). Folders are only virtual, and so not real, in Blob Storage.
func (l Location) IsFolderAware() bool {
	switch l {
	case ELocation.BlobFS(), ELocation.File(), ELocation.Local(), ELocation.FileNFS():
		return true
	case ELocation.Blob(), ELocation.S3(), ELocation.GCP(), ELocation.Benchmark(), ELocation.Pipe(), ELocation.Unknown(), ELocation.None():
		return false
	default:
		panic("unexpected location, please specify if it is folder-aware")
	}
}

func (l Location) CanForwardOAuthTokens() bool {
	return l == ELocation.Blob() || l == ELocation.BlobFS() || l == ELocation.File() || l == ELocation.FileNFS()
}

func (l Location) SupportsHnsACLs() bool {
	return l == ELocation.Blob() || l == ELocation.BlobFS()
}

func (l Location) IsFile() bool {
	return l == ELocation.File() || l == ELocation.FileNFS()
}

func (l Location) SupportsTrailingDot() bool {
	if (l == ELocation.File() || l == ELocation.FileNFS()) || (l == ELocation.Local() && runtime.GOOS != "windows") {
		return true
	}

	return false
}

func (ft FromTo) IsRedirection() bool {
	return ft == EFromTo.PipeBlob() || ft == EFromTo.BlobPipe()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EFromTo = FromTo(0)

// FromTo defines the different types of sources/destination location combinations
// FromTo is 16 bit where first 8 bit represents the from location and other 8 bits
// represents the to location
type FromTo uint16

func (FromTo) Unknown() FromTo { return FromTo(0) }

func (FromTo) LocalBlob() FromTo      { return FromToValue(ELocation.Local(), ELocation.Blob()) }
func (FromTo) LocalFile() FromTo      { return FromToValue(ELocation.Local(), ELocation.File()) }
func (FromTo) BlobLocal() FromTo      { return FromToValue(ELocation.Blob(), ELocation.Local()) }
func (FromTo) FileLocal() FromTo      { return FromToValue(ELocation.File(), ELocation.Local()) }
func (FromTo) BlobPipe() FromTo       { return FromToValue(ELocation.Blob(), ELocation.Pipe()) }
func (FromTo) PipeBlob() FromTo       { return FromToValue(ELocation.Pipe(), ELocation.Blob()) }
func (FromTo) FilePipe() FromTo       { return FromToValue(ELocation.File(), ELocation.Pipe()) }
func (FromTo) FileSMBPipe() FromTo    { return FromToValue(ELocation.File(), ELocation.Pipe()) }
func (FromTo) PipeFile() FromTo       { return FromToValue(ELocation.Pipe(), ELocation.File()) }
func (FromTo) PipeFileSMB() FromTo    { return FromToValue(ELocation.Pipe(), ELocation.File()) }
func (FromTo) BlobTrash() FromTo      { return FromToValue(ELocation.Blob(), ELocation.Unknown()) }
func (FromTo) FileTrash() FromTo      { return FromToValue(ELocation.File(), ELocation.Unknown()) }
func (FromTo) FileSMBTrash() FromTo   { return FromToValue(ELocation.File(), ELocation.Unknown()) }
func (FromTo) BlobFSTrash() FromTo    { return FromToValue(ELocation.BlobFS(), ELocation.Unknown()) }
func (FromTo) LocalBlobFS() FromTo    { return FromToValue(ELocation.Local(), ELocation.BlobFS()) }
func (FromTo) BlobFSLocal() FromTo    { return FromToValue(ELocation.BlobFS(), ELocation.Local()) }
func (FromTo) BlobFSBlobFS() FromTo   { return FromToValue(ELocation.BlobFS(), ELocation.BlobFS()) }
func (FromTo) BlobFSBlob() FromTo     { return FromToValue(ELocation.BlobFS(), ELocation.Blob()) }
func (FromTo) BlobFSFile() FromTo     { return FromToValue(ELocation.BlobFS(), ELocation.File()) }
func (FromTo) BlobFSFileSMB() FromTo  { return FromToValue(ELocation.BlobFS(), ELocation.File()) }
func (FromTo) BlobBlobFS() FromTo     { return FromToValue(ELocation.Blob(), ELocation.BlobFS()) }
func (FromTo) FileBlobFS() FromTo     { return FromToValue(ELocation.File(), ELocation.BlobFS()) }
func (FromTo) FileSMBBlobFS() FromTo  { return FromToValue(ELocation.File(), ELocation.BlobFS()) }
func (FromTo) BlobBlob() FromTo       { return FromToValue(ELocation.Blob(), ELocation.Blob()) }
func (FromTo) FileBlob() FromTo       { return FromToValue(ELocation.File(), ELocation.Blob()) }
func (FromTo) FileSMBBlob() FromTo    { return FromToValue(ELocation.File(), ELocation.Blob()) }
func (FromTo) BlobFile() FromTo       { return FromToValue(ELocation.Blob(), ELocation.File()) }
func (FromTo) BlobFileSMB() FromTo    { return FromToValue(ELocation.Blob(), ELocation.File()) }
func (FromTo) FileFile() FromTo       { return FromToValue(ELocation.File(), ELocation.File()) }
func (FromTo) S3Blob() FromTo         { return FromToValue(ELocation.S3(), ELocation.Blob()) }
func (FromTo) GCPBlob() FromTo        { return FromToValue(ELocation.GCP(), ELocation.Blob()) }
func (FromTo) BlobNone() FromTo       { return FromToValue(ELocation.Blob(), ELocation.None()) }
func (FromTo) BlobFSNone() FromTo     { return FromToValue(ELocation.BlobFS(), ELocation.None()) }
func (FromTo) FileNone() FromTo       { return FromToValue(ELocation.File(), ELocation.None()) }
func (FromTo) LocalFileNFS() FromTo   { return FromToValue(ELocation.Local(), ELocation.FileNFS()) }
func (FromTo) FileNFSLocal() FromTo   { return FromToValue(ELocation.FileNFS(), ELocation.Local()) }
func (FromTo) FileNFSFileNFS() FromTo { return FromToValue(ELocation.FileNFS(), ELocation.FileNFS()) }
func (FromTo) LocalFileSMB() FromTo   { return FromToValue(ELocation.Local(), ELocation.File()) }
func (FromTo) FileSMBLocal() FromTo   { return FromToValue(ELocation.File(), ELocation.Local()) }
func (FromTo) FileSMBFileSMB() FromTo { return FromToValue(ELocation.File(), ELocation.File()) }
func (FromTo) FileSMBFileNFS() FromTo { return FromToValue(ELocation.File(), ELocation.FileNFS()) }
func (FromTo) FileNFSFileSMB() FromTo { return FromToValue(ELocation.FileNFS(), ELocation.File()) }

// todo: to we really want these?  Starts to look like a bit of a combinatorial explosion
func (FromTo) BenchmarkBlob() FromTo {
	return FromTo(FromToValue(ELocation.Benchmark(), ELocation.Blob()))
}
func (FromTo) BenchmarkFile() FromTo {
	return FromTo(FromToValue(ELocation.Benchmark(), ELocation.File()))
}
func (FromTo) BenchmarkFileNFS() FromTo {
	return FromTo(FromToValue(ELocation.Benchmark(), ELocation.FileNFS()))
}

func (FromTo) BenchmarkBlobFS() FromTo {
	return FromTo(FromToValue(ELocation.Benchmark(), ELocation.BlobFS()))
}

func (ft FromTo) String() string {
	return enum.StringInt(ft, reflect.TypeOf(ft))
}
func (ft *FromTo) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ft), s, true, true)
	if err == nil {
		*ft = val.(FromTo)
	}
	return err
}

func (ft FromTo) FromAndTo(s string) (srcLocation, dstLocation Location, err error) {
	srcLocation = ELocation.Unknown()
	dstLocation = ELocation.Unknown()
	val, err := enum.ParseInt(reflect.TypeOf(ft), s, true, true)
	if err == nil {
		dstLocation = Location(((1 << 8) - 1) & val.(FromTo))
		srcLocation = Location((((1 << 16) - 1) & val.(FromTo)) >> 8)
		return
	}
	err = fmt.Errorf("unable to parse the from and to Location from given FromTo %s", s)
	return
}

func (ft FromTo) To() Location {
	return Location(((1 << 8) - 1) & ft)
}

func (ft FromTo) From() Location {
	return Location((((1 << 16) - 1) & ft) >> 8)
}

func (ft FromTo) IsDownload() bool {
	return ft.From().IsRemote() && ft.To().IsLocal() && ft.To() != ELocation.None() && ft.To() != ELocation.Unknown()
}

func (ft FromTo) IsS2S() bool {
	return ft.From().IsRemote() && ft.To().IsRemote() && ft.To() != ELocation.None() && ft.To() != ELocation.Unknown()
}

func (ft FromTo) IsNFS() bool {
	return ft.From() == ELocation.FileNFS() || ft.To() == ELocation.FileNFS()
}

func (ft FromTo) IsUpload() bool {
	return ft.From().IsLocal() && ft.To().IsRemote() && ft.To() != ELocation.None() && ft.To() != ELocation.Unknown()
}

func (ft FromTo) IsDelete() bool {
	return ft.To() == ELocation.Unknown()
}

func (ft FromTo) IsSetProperties() bool {
	return ft.To() == ELocation.None()
}

func (ft FromTo) AreBothFolderAware() bool {
	return ft.From().IsFolderAware() && ft.To().IsFolderAware()
}

func (ft FromTo) BothSupportTrailingDot() bool {
	return ft.From().SupportsTrailingDot() && ft.To().SupportsTrailingDot()
}

func (ft FromTo) IsPropertyOnlyTransfer() bool {
	return ft == EFromTo.BlobNone() || ft == EFromTo.BlobFSNone() || ft == EFromTo.FileNone()
}

// TODO: deletes are not covered by the above Is* routines

var BenchmarkLmt = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Enumerates the values for blob type.
type BlobType uint8

var EBlobType = BlobType(0)

func (BlobType) Detect() BlobType { return BlobType(0) }

func (BlobType) BlockBlob() BlobType { return BlobType(1) }

func (BlobType) PageBlob() BlobType { return BlobType(2) }

func (BlobType) AppendBlob() BlobType { return BlobType(3) }

func (bt BlobType) String() string {
	return enum.StringInt(bt, reflect.TypeOf(bt))
}

func (bt *BlobType) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(bt), s, true, true)
	if err == nil {
		*bt = val.(BlobType)
	}
	return err
}

func FromBlobType(bt blob.BlobType) BlobType {
	switch bt {
	case blob.BlobTypeBlockBlob:
		return EBlobType.BlockBlob()
	case blob.BlobTypePageBlob:
		return EBlobType.PageBlob()
	case blob.BlobTypeAppendBlob:
		return EBlobType.AppendBlob()
	default:
		return EBlobType.Detect()
	}
}

// ToBlobType returns the equivalent blob.BlobType for given string.
func (bt *BlobType) ToBlobType() blob.BlobType {
	blobType := bt.String()
	switch blobType {
	case string(blob.BlobTypeBlockBlob):
		return blob.BlobTypeBlockBlob
	case string(blob.BlobTypePageBlob):
		return blob.BlobTypePageBlob
	case string(blob.BlobTypeAppendBlob):
		return blob.BlobTypeAppendBlob
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var ETransferStatus = TransferStatus(0)

type TransferStatus int32 // Must be 32-bit for atomic operations; negative #s represent a specific failure code

func (t TransferStatus) StatusLocked() bool { // Is an overwrite necessary to change tx status?
	// Any kind of failure, or success is considered "locked in".
	return t <= ETransferStatus.Failed() || t == ETransferStatus.Success()
}

// Transfer is ready to transfer and not started transferring yet
func (TransferStatus) NotStarted() TransferStatus { return TransferStatus(0) }

// TODO confirm whether this is actually needed
//
//	Outdated:
//	  Transfer started & at least 1 chunk has successfully been transferred.
//	  Used to resume a transfer that started to avoid transferring all chunks thereby improving performance
//
// Update(Jul 2020): This represents the state of transfer as soon as the file is scheduled.
func (TransferStatus) Started() TransferStatus { return TransferStatus(1) }

// Transfer successfully completed
func (TransferStatus) Success() TransferStatus { return TransferStatus(2) }

// Folder was created, but properties have not been persisted yet. Equivalent to Started, but never intended to be set on anything BUT folders.
func (TransferStatus) FolderCreated() TransferStatus { return TransferStatus(3) }

func (TransferStatus) Restarted() TransferStatus { return TransferStatus(4) }

// Transfer failed due to some error.
func (TransferStatus) Failed() TransferStatus { return TransferStatus(-1) }

// Transfer failed due to failure while Setting blob tier.
func (TransferStatus) BlobTierFailure() TransferStatus { return TransferStatus(-2) }

func (TransferStatus) SkippedEntityAlreadyExists() TransferStatus { return TransferStatus(-3) }

func (TransferStatus) SkippedBlobHasSnapshots() TransferStatus { return TransferStatus(-4) }

func (TransferStatus) TierAvailabilityCheckFailure() TransferStatus { return TransferStatus(-5) }

func (TransferStatus) Cancelled() TransferStatus { return TransferStatus(-6) }

// Transfer is any of the three possible state (InProgress, Completer or Failed)
func (TransferStatus) All() TransferStatus { return TransferStatus(math.MaxInt8) }
func (ts TransferStatus) String() string {
	return enum.StringInt(ts, reflect.TypeOf(ts))
}
func (ts *TransferStatus) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ts), s, false, true)
	if err == nil {
		*ts = val.(TransferStatus)
	}
	return err
}

// Implementing MarshalJSON() method for type Transfer Status
func (ts TransferStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(ts.String())
}

// Implementing UnmarshalJSON() method for type Transfer Status
func (ts *TransferStatus) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return ts.Parse(s)
}

func (ts *TransferStatus) AtomicLoad() TransferStatus {
	return TransferStatus(atomic.LoadInt32((*int32)(ts)))
}
func (ts *TransferStatus) AtomicStore(newTransferStatus TransferStatus) {
	atomic.StoreInt32((*int32)(ts), int32(newTransferStatus))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EBlockBlobTier = BlockBlobTier(0)

type BlockBlobTier uint8

func (BlockBlobTier) None() BlockBlobTier    { return BlockBlobTier(0) }
func (BlockBlobTier) Hot() BlockBlobTier     { return BlockBlobTier(1) }
func (BlockBlobTier) Cool() BlockBlobTier    { return BlockBlobTier(2) }
func (BlockBlobTier) Archive() BlockBlobTier { return BlockBlobTier(3) }
func (BlockBlobTier) Cold() BlockBlobTier    { return BlockBlobTier(4) }

func (bbt BlockBlobTier) String() string {
	return enum.StringInt(bbt, reflect.TypeOf(bbt))
}

func (bbt *BlockBlobTier) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(bbt), s, true, true)
	if err == nil {
		*bbt = val.(BlockBlobTier)
	}
	return err
}

func (bbt BlockBlobTier) ToAccessTierType() blob.AccessTier {
	return blob.AccessTier(bbt.String())
}

func (bbt BlockBlobTier) MarshalJSON() ([]byte, error) {
	return json.Marshal(bbt.String())
}

// Implementing UnmarshalJSON() method for type BlockBlobTier.
func (bbt *BlockBlobTier) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return bbt.Parse(s)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EPageBlobTier = PageBlobTier(0)

type PageBlobTier uint8

func (PageBlobTier) None() PageBlobTier { return PageBlobTier(0) }
func (PageBlobTier) P10() PageBlobTier  { return PageBlobTier(10) }
func (PageBlobTier) P15() PageBlobTier  { return PageBlobTier(15) }
func (PageBlobTier) P20() PageBlobTier  { return PageBlobTier(20) }
func (PageBlobTier) P30() PageBlobTier  { return PageBlobTier(30) }
func (PageBlobTier) P4() PageBlobTier   { return PageBlobTier(4) }
func (PageBlobTier) P40() PageBlobTier  { return PageBlobTier(40) }
func (PageBlobTier) P50() PageBlobTier  { return PageBlobTier(50) }
func (PageBlobTier) P6() PageBlobTier   { return PageBlobTier(6) }

func (pbt PageBlobTier) String() string {
	return enum.StringInt(pbt, reflect.TypeOf(pbt))
}

func (pbt *PageBlobTier) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(pbt), s, true, true)
	if err == nil {
		*pbt = val.(PageBlobTier)
	}
	return err
}

func (pbt PageBlobTier) ToAccessTierType() blob.AccessTier {
	return blob.AccessTier(pbt.String())
}

func (pbt PageBlobTier) MarshalJSON() ([]byte, error) {
	return json.Marshal(pbt.String())
}

// Implementing UnmarshalJSON() method for type BlockBlobTier.
func (pbt *PageBlobTier) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return pbt.Parse(s)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var ECredentialType = CredentialType(0)

// CredentialType defines the different types of credentials
type CredentialType uint8

func (CredentialType) Unknown() CredentialType              { return CredentialType(0) }
func (CredentialType) OAuthToken() CredentialType           { return CredentialType(1) } // For Azure, OAuth
func (CredentialType) MDOAuthToken() CredentialType         { return CredentialType(7) } // For Azure MD impexp
func (CredentialType) Anonymous() CredentialType            { return CredentialType(2) } // For Azure, SAS or public.
func (CredentialType) SharedKey() CredentialType            { return CredentialType(3) } // For Azure, SharedKey
func (CredentialType) S3AccessKey() CredentialType          { return CredentialType(4) } // For S3, AccessKeyID and SecretAccessKey
func (CredentialType) GoogleAppCredentials() CredentialType { return CredentialType(5) } // For GCP, App Credentials
func (CredentialType) S3PublicBucket() CredentialType       { return CredentialType(6) } // For S3, Anon Credentials & public bucket

func (ct CredentialType) IsAzureOAuth() bool {
	return ct == ct.OAuthToken() || ct == ct.MDOAuthToken()
}

func (ct CredentialType) IsSharedKey() bool {
	return ct == ct.SharedKey()
}

func (ct CredentialType) String() string {
	return enum.StringInt(ct, reflect.TypeOf(ct))
}
func (ct *CredentialType) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ct), s, true, true)
	if err == nil {
		*ct = val.(CredentialType)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EHashValidationOption = HashValidationOption(0)

var DefaultHashValidationOption = EHashValidationOption.FailIfDifferent()

type HashValidationOption uint8

// FailIfDifferent says fail if hashes different, but NOT fail if saved hash is
// totally missing. This is a balance of convenience (for cases where no hash is saved) vs strictness
// (to validate strictly when one is present)
func (HashValidationOption) FailIfDifferent() HashValidationOption { return HashValidationOption(0) }

// Do not check hashes at download time at all
func (HashValidationOption) NoCheck() HashValidationOption { return HashValidationOption(1) }

// LogOnly means only log if missing or different, don't fail the transfer
func (HashValidationOption) LogOnly() HashValidationOption { return HashValidationOption(2) }

// FailIfDifferentOrMissing is the strictest option, and useful for testing or validation in cases when
// we _know_ there should be a hash
func (HashValidationOption) FailIfDifferentOrMissing() HashValidationOption {
	return HashValidationOption(3)
}

func (hvo HashValidationOption) String() string {
	return enum.StringInt(hvo, reflect.TypeOf(hvo))
}

func (hvo *HashValidationOption) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(hvo), s, true, true)
	if err == nil {
		*hvo = val.(HashValidationOption)
	}
	return err
}

func (hvo HashValidationOption) MarshalJSON() ([]byte, error) {
	return json.Marshal(hvo.String())
}

func (hvo *HashValidationOption) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return hvo.Parse(s)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EInvalidMetadataHandleOption = InvalidMetadataHandleOption(0)

var DefaultInvalidMetadataHandleOption = EInvalidMetadataHandleOption.ExcludeIfInvalid()

type InvalidMetadataHandleOption uint8

// ExcludeIfInvalid indicates whenever invalid metadata key is found, exclude the specific metadata with WARNING logged.
func (InvalidMetadataHandleOption) ExcludeIfInvalid() InvalidMetadataHandleOption {
	return InvalidMetadataHandleOption(0)
}

// FailIfInvalid indicates whenever invalid metadata key is found, directly fail the transfer.
func (InvalidMetadataHandleOption) FailIfInvalid() InvalidMetadataHandleOption {
	return InvalidMetadataHandleOption(1)
}

// RenameIfInvalid indicates whenever invalid metadata key is found, rename the metadata key and save the metadata with renamed key.
func (InvalidMetadataHandleOption) RenameIfInvalid() InvalidMetadataHandleOption {
	return InvalidMetadataHandleOption(2)
}

func (i InvalidMetadataHandleOption) String() string {
	return enum.StringInt(i, reflect.TypeOf(i))
}

func (i *InvalidMetadataHandleOption) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(i), s, true, true)
	if err == nil {
		*i = val.(InvalidMetadataHandleOption)
	}
	return err
}

func (i InvalidMetadataHandleOption) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.String())
}

func (i *InvalidMetadataHandleOption) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return i.Parse(s)
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
const (
	DefaultBlockBlobBlockSize      = 8 * 1024 * 1024
	MaxBlockBlobBlockSize          = 4000 * 1024 * 1024
	MaxPutBlobSize                 = 5000 * 1024 * 1024
	MaxAppendBlobBlockSize         = 100 * 1024 * 1024
	DefaultPageBlobChunkSize       = 4 * 1024 * 1024
	DefaultAzureFileChunkSize      = 4 * 1024 * 1024
	MaxRangeGetSize                = 4 * 1024 * 1024
	MaxNumberOfBlocksPerBlob       = 50000
	BlockSizeThreshold             = 256 * 1024 * 1024
	MinParallelChunkCountThreshold = 4 /* minimum number of chunks in parallel for AzCopy to be performant. */
	GigaByte                       = 1024 * 1024 * 1024
	MegaByte                       = 1024 * 1024
	KiloByte                       = 1024
)

// This struct represent a single transfer entry with source and destination details
// ** DO NOT construct directly. Use cmd.storedObject.ToNewCopyTransfer **
type CopyTransfer struct {
	Source           string
	Destination      string
	EntityType       EntityType
	LastModifiedTime time.Time // represents the last modified time of source which ensures that source hasn't changed while transferring
	SourceSize       int64     // size of the source entity in bytes.

	// Properties for service to service copy (some also used in upload or download too)
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	ContentLanguage    string
	CacheControl       string
	ContentMD5         []byte
	Metadata           Metadata

	// Properties for S2S blob copy
	BlobType      blob.BlobType
	BlobTier      blob.AccessTier
	BlobVersionID string
	// Blob index tags categorize data in your storage account utilizing key-value tag attributes
	BlobTags BlobTags

	BlobSnapshotID     string
	TargetHardlinkFile string // used only for NFS transfers to indicate the target hardlink file path
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Metadata used in AzCopy.
const MetadataAndBlobTagsClearFlag = "clear" // clear flag used for metadata and tags

type Metadata map[string]*string

func (m Metadata) Clone() Metadata {
	out := make(Metadata)

	for k, v := range m {
		out[k] = v
	}

	return out
}

// Marshal marshals metadata to string.
func (m Metadata) Marshal() (string, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// UnMarshalToCommonMetadata unmarshals string to common metadata.
func UnMarshalToCommonMetadata(metadataString string) (Metadata, error) {
	var result Metadata
	if metadataString != "" {
		err := json.Unmarshal([]byte(metadataString), &result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func StringToMetadata(metadataString string) (Metadata, error) {
	metadataMap := Metadata{}
	if len(metadataString) > 0 {
		cKey := ""
		cVal := ""
		keySet := false
		ignoreRules := false

		addchar := func(c rune) {
			if !keySet {
				cKey += string(c)
			} else {
				cVal += string(c)
			}
		}
		for _, c := range metadataString {
			if ignoreRules {
				addchar(c)
				ignoreRules = false
			} else {
				switch c {
				case '=':
					if keySet {
						addchar(c)
					} else {
						keySet = true
					}

				case ';':
					if !keySet {
						return Metadata{}, errors.New("metadata names must conform to C# naming rules (https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#metadata-names)")
					}

					finalValue := cVal
					metadataMap[cKey] = &finalValue
					cKey = ""
					cVal = ""
					keySet = false
					ignoreRules = false

				case '\\':
					ignoreRules = true // ignore the rules on the next character

				default:
					addchar(c)
				}
			}
		}

		if cKey != "" {
			finalValue := cVal
			metadataMap[cKey] = &finalValue
		}
	}
	return metadataMap, nil
}

// isValidMetadataKey checks if the given string is a valid metadata key for Azure.
// For Azure, metadata key must adhere to the naming rules for C# identifiers.
// As testing, reserved keywords for C# identifiers are also valid metadata key. (e.g. this, int)
// TODO: consider to use "[A-Za-z_]\w*" to replace this implementation, after ensuring the complexity is O(N).
func isValidMetadataKey(key string) bool {
	for i := 0; i < len(key); i++ {
		if i != 0 { // Most of case i != 0
			if !isValidMetadataKeyChar(key[i]) {
				return false
			}
			// Coming key is valid
		} else { // i == 0
			if !isValidMetadataKeyFirstChar(key[i]) {
				return false
			}
			// First key is valid
		}
	}

	return true
}

func isValidMetadataKeyChar(c byte) bool {
	if (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' {
		return true
	}
	return false
}

func isValidMetadataKeyFirstChar(c byte) bool {
	if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' {
		return true
	}
	return false
}

func (m Metadata) ExcludeInvalidKey() (retainedMetadata Metadata, excludedMetadata Metadata, invalidKeyExists bool) {
	retainedMetadata = make(map[string]*string)
	excludedMetadata = make(map[string]*string)
	for k, v := range m {
		if isValidMetadataKey(k) {
			retainedMetadata[k] = v
		} else {
			invalidKeyExists = true
			excludedMetadata[k] = v
		}
	}

	return
}

// BlobTags is a map of key-value pair
type BlobTags map[string]string

func (bt BlobTags) ToString() string {
	lst := make([]string, 0)
	for k, v := range bt {
		lst = append(lst, k+"="+v)
	}
	return strings.Join(lst, "&")
}

func ToCommonBlobTagsMap(blobTagsString string) BlobTags {
	if blobTagsString == "" { // default empty value set by coder
		return nil
	}
	if strings.EqualFold(blobTagsString, MetadataAndBlobTagsClearFlag) { // "clear" value given by user as input (to signify clearing of tags in set-props cmd)
		return BlobTags{}
	}

	blobTagsMap := BlobTags{}
	for _, keyAndValue := range strings.Split(blobTagsString, "&") { // key/value pairs are separated by '&'
		kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
		blobTagsMap[kv[0]] = kv[1]
	}
	return blobTagsMap
}

const metadataRenamedKeyPrefix = "rename_"
const metadataKeyForRenamedOriginalKeyPrefix = "rename_key_"

var metadataKeyInvalidCharRegex = regexp.MustCompile(`\W`)
var metadataKeyRenameErrStr = "failed to rename invalid metadata key %q"

// ResolveInvalidKey resolves invalid metadata key with following steps:
// 1. replace all invalid char(i.e. ASCII chars expect [0-9A-Za-z_]) with '_'
// 2. add 'rename_' as prefix for the new valid key, this key will be used to save original metadata's value.
// 3. add 'rename_key_' as prefix for the new valid key, this key will be used to save original metadata's invalid key.
// Example, given invalid metadata for Azure: '123-invalid':'content', it will be resolved as two new k:v pairs:
// 'rename_123_invalid':'content'
// 'rename_key_123_invalid':'123-invalid'
// So user can try to recover the metadata in Azure side.
// Note: To keep first version simple, whenever collision is found during key resolving, error will be returned.
// This can be further improved once any user feedback get.
func (m Metadata) ResolveInvalidKey() (resolvedMetadata Metadata, err error) {
	resolvedMetadata = make(map[string]*string)

	hasCollision := func(name string) bool {
		_, hasCollisionToOrgNames := m[name]
		_, hasCollisionToNewNames := resolvedMetadata[name]

		return hasCollisionToOrgNames || hasCollisionToNewNames
	}

	for k, v := range m {
		value := v
		valueString := &value
		key := k
		keyString := &key
		if !isValidMetadataKey(k) {
			validKey := metadataKeyInvalidCharRegex.ReplaceAllString(k, "_")
			renamedKey := metadataRenamedKeyPrefix + validKey
			keyForRenamedOriginalKey := metadataKeyForRenamedOriginalKeyPrefix + validKey
			if hasCollision(renamedKey) || hasCollision(keyForRenamedOriginalKey) {
				return nil, fmt.Errorf(metadataKeyRenameErrStr, *keyString)
			}

			resolvedMetadata[renamedKey] = *valueString
			resolvedMetadata[keyForRenamedOriginalKey] = keyString
		} else {
			resolvedMetadata[k] = *valueString
		}
	}

	return resolvedMetadata, nil
}

func (m Metadata) ConcatenatedKeys() string {
	buf := bytes.Buffer{}

	for k := range m {
		buf.WriteString("'")
		buf.WriteString(k)
		buf.WriteString("' ")
	}

	return buf.String()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Common resource's HTTP headers stands for properties used in AzCopy.
type ResourceHTTPHeaders struct {
	ContentType        string
	ContentMD5         []byte
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
}

// ToBlobHTTPHeaders converts ResourceHTTPHeaders to blob's HTTPHeaders.
func (h ResourceHTTPHeaders) ToBlobHTTPHeaders() blob.HTTPHeaders {
	return blob.HTTPHeaders{
		BlobContentType:        IffNotEmpty(h.ContentType),
		BlobContentMD5:         h.ContentMD5,
		BlobContentEncoding:    IffNotEmpty(h.ContentEncoding),
		BlobContentLanguage:    IffNotEmpty(h.ContentLanguage),
		BlobContentDisposition: IffNotEmpty(h.ContentDisposition),
		BlobCacheControl:       IffNotEmpty(h.CacheControl),
	}
}

// ToFileHTTPHeaders converts ResourceHTTPHeaders to sharefile's HTTPHeaders.
func (h ResourceHTTPHeaders) ToFileHTTPHeaders() sharefile.HTTPHeaders {
	return sharefile.HTTPHeaders{
		ContentType:        IffNotEmpty(h.ContentType),
		ContentMD5:         h.ContentMD5,
		ContentEncoding:    IffNotEmpty(h.ContentEncoding),
		ContentLanguage:    IffNotEmpty(h.ContentLanguage),
		ContentDisposition: IffNotEmpty(h.ContentDisposition),
		CacheControl:       IffNotEmpty(h.CacheControl),
	}
}

// ToBlobFSHTTPHeaders converts ResourceHTTPHeaders to BlobFS Headers.
func (h ResourceHTTPHeaders) ToBlobFSHTTPHeaders() datalakefile.HTTPHeaders {
	return datalakefile.HTTPHeaders{
		ContentType:        IffNotEmpty(h.ContentType),
		ContentMD5:         h.ContentMD5,
		ContentEncoding:    IffNotEmpty(h.ContentEncoding),
		ContentLanguage:    IffNotEmpty(h.ContentLanguage),
		ContentDisposition: IffNotEmpty(h.ContentDisposition),
		CacheControl:       IffNotEmpty(h.CacheControl),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var ETransferDirection = TransferDirection(0)

type TransferDirection int32

func (TransferDirection) UnKnown() TransferDirection  { return TransferDirection(0) }
func (TransferDirection) Upload() TransferDirection   { return TransferDirection(1) }
func (TransferDirection) Download() TransferDirection { return TransferDirection(2) }
func (TransferDirection) S2SCopy() TransferDirection  { return TransferDirection(3) }

func (td TransferDirection) String() string {
	return enum.StringInt(td, reflect.TypeOf(td))
}
func (td *TransferDirection) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(td), s, false, true)
	if err == nil {
		*td = val.(TransferDirection)
	}
	return err
}

func (td *TransferDirection) AtomicLoad() TransferDirection {
	return TransferDirection(atomic.LoadInt32((*int32)(td)))
}
func (td *TransferDirection) AtomicStore(newTransferDirection TransferDirection) {
	atomic.StoreInt32((*int32)(td), int32(newTransferDirection))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EPerfConstraint = PerfConstraint(0)

type PerfConstraint int32

func (PerfConstraint) Unknown() PerfConstraint         { return PerfConstraint(0) }
func (PerfConstraint) Disk() PerfConstraint            { return PerfConstraint(1) }
func (PerfConstraint) Service() PerfConstraint         { return PerfConstraint(2) }
func (PerfConstraint) PageBlobService() PerfConstraint { return PerfConstraint(3) }
func (PerfConstraint) CPU() PerfConstraint             { return PerfConstraint(4) }

// others will be added in future

func (pc PerfConstraint) String() string {
	return enum.StringInt(pc, reflect.TypeOf(pc))
}

func (pc *PerfConstraint) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(pc), s, false, true)
	if err == nil {
		*pc = val.(PerfConstraint)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EHardlinkHandlingType = HardlinkHandlingType(0)

var DefaultHardlinkHandlingType = EHardlinkHandlingType.Follow()
var SkipHardlinkHandlingType = EHardlinkHandlingType.Skip()

type HardlinkHandlingType uint8

// Follow means copy the files to the destination as regular files
func (HardlinkHandlingType) Follow() HardlinkHandlingType {
	return HardlinkHandlingType(0)
}

// Skip means skip the hardlinks and do not copy them to the destination
func (HardlinkHandlingType) Skip() HardlinkHandlingType {
	return HardlinkHandlingType(1)
}

func (HardlinkHandlingType) Preserve() HardlinkHandlingType {
	return HardlinkHandlingType(2)
}

func (pho HardlinkHandlingType) String() string {
	return enum.StringInt(pho, reflect.TypeOf(pho))
}

func (pho *HardlinkHandlingType) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(pho), s, true, true)
	if err == nil {
		*pho = val.(HardlinkHandlingType)
	}
	return err
}

func (pho HardlinkHandlingType) MarshalJSON() ([]byte, error) {
	return json.Marshal(pho.String())
}

func (pho *HardlinkHandlingType) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	return pho.Parse(s)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type PerformanceAdvice struct {

	// Code representing the type of the advice
	Code string `json:"Code"` // reminder that PerformanceAdvice may be serialized in JSON output

	// Human-friendly title (directly corresponds to Code, but more readable)
	Title string

	// Reason why this advice has been given
	Reason string

	// Is this the primary advice (used to distinguish most important advice in cases where multiple advice objects are returned)
	PriorityAdvice bool
}

const BenchmarkPreviewNotice = "The benchmark feature is currently in Preview status."

const BenchmarkFinalDisclaimer = `This benchmark tries to find optimal performance, computed without touching any local disk. When reading 
and writing real data, performance may be different. If disk limits throughput, AzCopy will display a message on screen.`

const BenchmarkLinuxExtraDisclaimer = `On Linux, when AzCopy is uploading just one or two large files, disk performance may be greatly improved by
increasing read_ahead_kb to 8192 for the data disk.`

const SizePerFileParam = "size-per-file"
const FileCountParam = "file-count"
const FileCountDefault = 100

// BenchMarkMode enumerates values for Azcopy bench command. Valid values Upload or Download
type BenchMarkMode uint8

var EBenchMarkMode = BenchMarkMode(0)

func (BenchMarkMode) Upload() BenchMarkMode { return BenchMarkMode(0) }

func (BenchMarkMode) Download() BenchMarkMode { return BenchMarkMode(1) }

func (bm BenchMarkMode) String() string {
	return enum.StringInt(bm, reflect.TypeOf(bm))
}

func (bm *BenchMarkMode) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(bm), s, true, true)
	if err == nil {
		*bm = val.(BenchMarkMode)
	}
	return err
}

//////////////////////////////////////////////////////////////////////////////////////

var ECompressionType = CompressionType(0)

type CompressionType uint8

func (CompressionType) None() CompressionType        { return CompressionType(0) }
func (CompressionType) ZLib() CompressionType        { return CompressionType(1) }
func (CompressionType) GZip() CompressionType        { return CompressionType(2) }
func (CompressionType) Unsupported() CompressionType { return CompressionType(255) }

func (ct CompressionType) String() string {
	return enum.StringInt(ct, reflect.TypeOf(ct))
}

func GetCompressionType(contentEncoding string) (CompressionType, error) {
	switch strings.ToLower(contentEncoding) {
	case "":
		return ECompressionType.None(), nil
	case "gzip":
		return ECompressionType.GZip(), nil
	case "deflate":
		return ECompressionType.ZLib(), nil
	default:
		return ECompressionType.Unsupported(), fmt.Errorf("encoding type '%s' is not recognised as a supported encoding type for auto-decompression", contentEncoding)
	}
}

/////////////////////////////////////////////////////////////////

var EEntityType = EntityType(0)

type EntityType uint8

func (EntityType) File() EntityType           { return EntityType(0) }
func (EntityType) Folder() EntityType         { return EntityType(1) }
func (EntityType) Symlink() EntityType        { return EntityType(2) }
func (EntityType) FileProperties() EntityType { return EntityType(3) }
func (EntityType) Hardlink() EntityType       { return EntityType(4) }
func (EntityType) Other() EntityType          { return EntityType(5) }

func (e EntityType) String() string {
	return enum.StringInt(e, reflect.TypeOf(e))
}

func (e *EntityType) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(e), s, true, true)
	if err == nil {
		*e = val.(EntityType)
	}
	return err
}

////////////////////////////////////////////////////////////////

var EFolderPropertiesOption = FolderPropertyOption(0)

// FolderPropertyOption controls which folders get their properties recorded in the Plan file
type FolderPropertyOption uint8

// no FPO has been selected.  Make sure the zero-like value is "unspecified" so that we detect
// any code paths that that do not nominate any FPO
func (FolderPropertyOption) Unspecified() FolderPropertyOption { return FolderPropertyOption(0) }

func (FolderPropertyOption) NoFolders() FolderPropertyOption { return FolderPropertyOption(1) }
func (FolderPropertyOption) AllFoldersExceptRoot() FolderPropertyOption {
	return FolderPropertyOption(2)
}
func (FolderPropertyOption) AllFolders() FolderPropertyOption { return FolderPropertyOption(3) }

///////////////////////////////////////////////////////////////////////

var EPreservePermissionsOption = PreservePermissionsOption(0)

type PreservePermissionsOption uint8

func (PreservePermissionsOption) None() PreservePermissionsOption {
	return PreservePermissionsOption(0)
}
func (PreservePermissionsOption) ACLsOnly() PreservePermissionsOption {
	return PreservePermissionsOption(1)
}
func (PreservePermissionsOption) OwnershipAndACLs() PreservePermissionsOption {
	return PreservePermissionsOption(2)
}

func NewPreservePermissionsOption(preserve, includeOwnership bool, fromTo FromTo) PreservePermissionsOption {
	if preserve {
		if fromTo.IsDownload() {
			// downloads are the only time we respect includeOwnership
			if includeOwnership {
				return EPreservePermissionsOption.OwnershipAndACLs()
			} else {
				return EPreservePermissionsOption.ACLsOnly()
			}
		}
		// for uploads and S2S, we always include ownership
		return EPreservePermissionsOption.OwnershipAndACLs()
	}

	return EPreservePermissionsOption.None()
}

func (p PreservePermissionsOption) IsTruthy() bool {
	switch p {
	case EPreservePermissionsOption.ACLsOnly(),
		EPreservePermissionsOption.OwnershipAndACLs():
		return true
	case EPreservePermissionsOption.None():
		return false
	default:
		panic("unknown permissions option")
	}
}

func (p PreservePermissionsOption) IsOwner() bool {
	switch p {
	case EPreservePermissionsOption.OwnershipAndACLs():
		return true
	case EPreservePermissionsOption.ACLsOnly(), EPreservePermissionsOption.None():
		return false
	default:
		panic("unknown permissions option")
	}
}

type CpkOptions struct {
	// Optional flag to encrypt user data with user provided key.
	// Key is provide in the REST request itself
	// Provided key (EncryptionKey and EncryptionKeySHA256) and its hash will be fetched from environment variables
	// Set EncryptionAlgorithm = "AES256" by default.
	CpkInfo bool
	// Key is present in AzureKeyVault and Azure KeyVault is linked with storage account.
	// Provided key name will be fetched from Azure Key Vault and will be used to encrypt the data
	CpkScopeInfo string
	// flag to check if the source is encrypted by user provided key or not.
	// True only if user wishes to download source encrypted by user provided key
	IsSourceEncrypted bool
}

func (options CpkOptions) GetCPKInfo() (*blob.CPKInfo, error) {
	if !options.IsSourceEncrypted {
		return nil, nil
	} else {
		return GetCpkInfo(options.CpkInfo)
	}
}

func (options CpkOptions) GetCPKScopeInfo() *blob.CPKScopeInfo {
	if !options.IsSourceEncrypted {
		return nil
	} else {
		return GetCpkScopeInfo(options.CpkScopeInfo)
	}
}

// //////////////////////////////////////////////////////////////////////////////
type SetPropertiesFlags uint32 // [0000000000...32 times]

var ESetPropertiesFlags = SetPropertiesFlags(0)

// functions to set values
func (SetPropertiesFlags) None() SetPropertiesFlags        { return SetPropertiesFlags(0) }
func (SetPropertiesFlags) SetTier() SetPropertiesFlags     { return SetPropertiesFlags(1) }
func (SetPropertiesFlags) SetMetadata() SetPropertiesFlags { return SetPropertiesFlags(2) }
func (SetPropertiesFlags) SetBlobTags() SetPropertiesFlags { return SetPropertiesFlags(4) }

// functions to get values (to be used in sde)
// If Y is inside X then X & Y == Y
func (op *SetPropertiesFlags) ShouldTransferTier() bool {
	return (*op)&ESetPropertiesFlags.SetTier() == ESetPropertiesFlags.SetTier()
}
func (op *SetPropertiesFlags) ShouldTransferMetaData() bool {
	return (*op)&ESetPropertiesFlags.SetMetadata() == ESetPropertiesFlags.SetMetadata()
}
func (op *SetPropertiesFlags) ShouldTransferBlobTags() bool {
	return (*op)&ESetPropertiesFlags.SetBlobTags() == ESetPropertiesFlags.SetBlobTags()
}

// //////////////////////////////////////////////////////////////////////////////
type RehydratePriorityType uint8

var ERehydratePriorityType = RehydratePriorityType(0) // setting default as none

func (RehydratePriorityType) None() RehydratePriorityType     { return RehydratePriorityType(0) }
func (RehydratePriorityType) Standard() RehydratePriorityType { return RehydratePriorityType(1) }
func (RehydratePriorityType) High() RehydratePriorityType     { return RehydratePriorityType(2) }

func (rpt *RehydratePriorityType) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(rpt), s, true, true)
	if err == nil {
		*rpt = val.(RehydratePriorityType)
	}
	return err
}
func (rpt RehydratePriorityType) String() string {
	return enum.StringInt(rpt, reflect.TypeOf(rpt))
}

func (rpt RehydratePriorityType) ToRehydratePriorityType() blob.RehydratePriority {
	switch rpt {
	case ERehydratePriorityType.None(), ERehydratePriorityType.Standard():
		return blob.RehydratePriorityStandard
	case ERehydratePriorityType.High():
		return blob.RehydratePriorityHigh
	default:
		return blob.RehydratePriorityStandard
	}
}

// //////////////////////////////////////////////////////////////////////////////
type SyncHashType uint8

var ESyncHashType SyncHashType = 0

func (SyncHashType) None() SyncHashType {
	return 0
}

func (SyncHashType) MD5() SyncHashType {
	return 1
}

func (ht *SyncHashType) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(ht), s, true, true)
	if err == nil {
		*ht = val.(SyncHashType)
	}
	return err
}

func (ht SyncHashType) String() string {
	return enum.StringInt(ht, reflect.TypeOf(ht))
}

// //////////////////////////////////////////////////////////////////////////////
type SymlinkHandlingType uint8 // SymlinkHandlingType is only utilized internally to avoid having to carry around two contradictory flags. Thus, it doesn't have a parse method.

// for reviewers: This is different than we usually implement enums, but it's something I've found to be more pleasant in personal projects, especially for bitflags. Should we change the pattern to match this in the future?

type eSymlinkHandlingType uint8

var ESymlinkHandlingType = eSymlinkHandlingType(0)

func (eSymlinkHandlingType) Skip() SymlinkHandlingType     { return SymlinkHandlingType(0) }
func (eSymlinkHandlingType) Follow() SymlinkHandlingType   { return SymlinkHandlingType(1) } // Upload what's on the other hand of the symlink
func (eSymlinkHandlingType) Preserve() SymlinkHandlingType { return SymlinkHandlingType(2) } // Copy the link

func (sht SymlinkHandlingType) None() bool     { return sht == 0 }
func (sht SymlinkHandlingType) Follow() bool   { return sht == 1 }
func (sht SymlinkHandlingType) Preserve() bool { return sht == 2 }

func (sht *SymlinkHandlingType) Determine(Follow, Preserve bool) error {
	switch {
	case Follow && Preserve:
		return errors.New("cannot both follow and preserve symlinks (--preserve-symlinks and --follow-symlinks contradict)")
	case Preserve:
		*sht = ESymlinkHandlingType.Preserve()
	case Follow:
		*sht = ESymlinkHandlingType.Follow()
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var oncer = sync.Once{}

func WarnIfTooManyObjects() {
	oncer.Do(func() {
		GetLifecycleMgr().Warn(fmt.Sprintf("This job contains more than %d objects, best practice to run less than this.",
			RECOMMENDED_OBJECTS_COUNT))
	})
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
var EPosixPropertiesStyle = PosixPropertiesStyle(0)

var StandardPosixPropertiesStyle = EPosixPropertiesStyle.Standard() // Default
var AMLFSPosixPropertiesStyle = EPosixPropertiesStyle.AMLFS()

type PosixPropertiesStyle uint8

// Standard means use the default POSIX properties type
func (PosixPropertiesStyle) Standard() PosixPropertiesStyle {
	return PosixPropertiesStyle(0)
}

// AMLFS means use the Azure Managed Lustre File System POSIX attributes for owner, group ID, mode and modtime
func (PosixPropertiesStyle) AMLFS() PosixPropertiesStyle {
	return PosixPropertiesStyle(1)
}

func (ppt PosixPropertiesStyle) String() string {
	return enum.StringInt(ppt, reflect.TypeOf(ppt))
}

func (ppt *PosixPropertiesStyle) Parse(s string) error {
	if s == "" { // Default to standard when not set
		s = StandardPosixPropertiesStyle.String()
	}
	val, err := enum.ParseInt(reflect.TypeOf(ppt), s, true, true)
	if err == nil {
		*ppt = val.(PosixPropertiesStyle)
	}
	return err
}

////////////////////////////////////////////////////////////////

var EJobPartType = JobPartType(0)

// JobPartType defines the type of transfers a job part contains
type JobPartType uint8

func (JobPartType) Mixed() JobPartType    { return JobPartType(0) } // Default - mixed files,folders,symlinks
func (JobPartType) Hardlink() JobPartType { return JobPartType(1) } // Hardlinks only

func (jpt JobPartType) String() string {
	return enum.StringInt(jpt, reflect.TypeOf(jpt))
}

func (jpt *JobPartType) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(jpt), s, true, true)
	if err == nil {
		*jpt = val.(JobPartType)
	}
	return err
}

////////////////////////////////////////////////////////////////

var EJobProcessingMode = JobProcessingMode(0)

// JobProcessingMode defines how job parts should be processed
type JobProcessingMode uint8

func (JobProcessingMode) Mixed() JobProcessingMode { return JobProcessingMode(0) } // Default - process all job parts immediately
func (JobProcessingMode) NFS() JobProcessingMode   { return JobProcessingMode(1) } // Process file job parts first, then folder job parts

func (jpm JobProcessingMode) String() string {
	return enum.StringInt(jpm, reflect.TypeOf(jpm))
}
