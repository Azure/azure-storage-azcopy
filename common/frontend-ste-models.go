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
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"time"
	"net/http"
	"bytes"
	"io/ioutil"
)

type JobID UUID

func (j JobID) String() string {
	return UUID(j).String()
}

func ParseJobID(jobId string) (JobID, error){
	uuid, err := ParseUUID(jobId)
	if err != nil{
		return JobID{}, err
	}
	return JobID(uuid), nil
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

type PartNumber uint32
type Version uint32
type Status uint32

type TransferStatus uint32

func (status TransferStatus) String() (statusString string) {
	switch status {
	case TransferInProgress:
		return "InProgress"
	case TransferComplete:
		return "TransferComplete"
	case TransferFailed:
		return "TransferFailed"
	case TransferAny:
		return "TransferAny"
	default:
		return "InvalidStatusCode"
	}
}

const (
	// Transfer is currently executing or cancelled before failure or successful execution
	TransferInProgress TransferStatus = 0

	// Transfer has completed successfully
	TransferComplete TransferStatus = 1

	// Transfer has failed due to some error. This status does represent the state when transfer is cancelled
	TransferFailed TransferStatus = 2

	// Transfer is any of the three possible state (InProgress, Completer or Failed)
	TransferAny TransferStatus = TransferStatus(254)
)

// TransferStatusStringToCode converts the user given Transfer status string to the internal Transfer Status constants.
// TransferStatusStringToCode is used to avoid exposing the Transfer constants value to the user.
func TransferStatusStringToCode(statusString string) TransferStatus {
	switch statusString {
	case "TransferInProgress":
		return TransferInProgress
	case "TransferComplete":
		return TransferComplete
	case "TransferFailed":
		return TransferFailed
	case "TransferAny":
		return TransferAny
	default:
		panic(fmt.Errorf("invalid status string %s", statusString))
	}
}

type LogLevel pipeline.LogLevel

const (
	// LogNone tells a logger not to log any entries passed to it.
	LogNone = LogLevel(pipeline.LogNone)

	// LogFatal tells a logger to log all LogFatal entries passed to it.
	LogFatal = LogLevel(pipeline.LogFatal)

	// LogPanic tells a logger to log all LogPanic and LogFatal entries passed to it.
	LogPanic = LogLevel(pipeline.LogPanic)

	// LogError tells a logger to log all LogError, LogPanic and LogFatal entries passed to it.
	LogError = LogLevel(pipeline.LogError)

	// LogWarning tells a logger to log all LogWarning, LogError, LogPanic and LogFatal entries passed to it.
	LogWarning = LogLevel(pipeline.LogWarning)

	// LogInfo tells a logger to log all LogInfo, LogWarning, LogError, LogPanic and LogFatal entries passed to it.
	LogInfo = LogLevel(pipeline.LogInfo)
)

// String() converts the internal Loglevel constants to the user understandable strings.
func (logLevel LogLevel) String() string {
	switch logLevel {
	case LogNone:
		return "NoLogLevel"
	case LogFatal:
		return "FatalLogs"
	case LogPanic:
		return "PanicLogs"
	case LogError:
		return "ErrorLogs"
	case LogWarning:
		return "WarningLogs"
	case LogInfo:
		return "InfoLogs"
	default:
		panic(fmt.Errorf("invalid log level %d", logLevel))
	}
}

// represents the raw copy command input from the user
type CopyCmdArgsAndFlags struct {
	// from arguments
	Source                string
	Destination           string
	BlobUrlForRedirection string

	// inferred from arguments
	SourceType      LocationType
	DestinationType LocationType

	// filters from flags
	Include        string
	Exclude        string
	Recursive      bool
	FollowSymlinks bool
	WithSnapshots  bool

	// options from flags
	BlockSize                uint32
	BlobType                 string
	BlobTier                 string
	Metadata                 string
	ContentType              string
	ContentEncoding          string
	NoGuessMimeType          bool
	PreserveLastModifiedTime bool
	IsaBackgroundOp          bool
	Acl                      string
	LogVerbosity             uint8
}

// ListCmdArgsAndFlags represents the raw list command input from the user
type ListCmdArgsAndFlags struct {
	JobId    JobID
	OfStatus string
}

// define the different types of sources/destinations
type LocationType uint8

const (
	Local   LocationType = 0
	Blob    LocationType = 1
	Unknown LocationType = 2
)

// This struct represent a single transfer entry with source and destination details
type CopyTransfer struct {
	Source           string
	Destination      string
	LastModifiedTime time.Time //represents the last modified time of source which ensures that source hasn't changed while transferring
	SourceSize       int64     // size of the source entity in bytes
}

// This struct represents the job info (a single part) to be sent to the storage engine
type CopyJobPartOrderRequest struct {
	Version            uint32     // version of the azcopy
	ID                 JobID      // Guid - job identifier
	PartNum            PartNumber // part number of the job
	IsFinalPart        bool       // to determine the final part for a specific job
	Priority           uint8      // priority of the task
	SourceType         LocationType
	DestinationType    LocationType
	Transfers          []CopyTransfer
	LogVerbosity       LogLevel
	IsaBackgroundOp    bool
	OptionalAttributes BlobTransferAttributes
}

type CopyJobPartOrderResponse struct {
	Message string
}

// represents the raw list command input from the user when requested the list of transfer with given status for given JobId
type ListRequest struct {
	JobId                  string
	ExpectedTransferStatus TransferStatus
}

// This struct represents the optional attribute for blob request header
type BlobTransferAttributes struct {
	ContentType              string //The content type specified for the blob.
	ContentEncoding          string //Specifies which content encodings have been applied to the blob.
	Metadata                 string //User-defined name-value pairs associated with the blob
	NoGuessMimeType          bool   // represents user decision to interpret the content-encoding from source file
	PreserveLastModifiedTime bool   // when downloading, tell engine to set file's timestamp to timestamp of blob
	BlockSizeinBytes         uint32
}

// ExistingJobDetails represent the Job with JobId and
type ExistingJobDetails struct {
	JobIds []JobID
}

// represents the JobProgress Summary response for list command when requested the Job Progress Summary for given JobId
type JobProgressSummary struct {
	// CompleteJobOrdered determines whether the Job has been completely ordered or not
	CompleteJobOrdered             bool
	JobStatus                      string
	TotalNumberOfTransfers         uint32
	TotalNumberofTransferCompleted uint32
	TotalNumberofFailedTransfer    uint32
	//NumberOfTransferCompletedafterCheckpoint uint32
	//NumberOfTransferFailedAfterCheckpoint    uint32
	PercentageProgress          uint32
	FailedTransfers             []TransferDetail
	ThroughputInBytesPerSeconds float64
}

// represents the Details and details of a single transfer
type TransferDetail struct {
	Src            string
	Dst            string
	TransferStatus string
}
type HttpResponseMessage struct {
	Payload interface{}
	ErrorMsg string
}

type FooResponse struct {
	ErrorMsg string
}

// represents the list of Details and details of number of transfers
type TransfersDetail struct {
	Details []TransferDetail
}

// todo : use url in case of string
type HTTPClient struct {
	client *http.Client
	url    string
}

// NewHttpClient returns the instance of struct containing an instance of http.client and url
func NewHttpClient(url string) (*HTTPClient) {
	return &HTTPClient{
		client:      &http.Client{},
				url: url}
}

// Send method on HttpClient sends the data passed in the interface for given command type to the client url
func (httpClient *HTTPClient) Send(commandType string, v interface{}) ([] byte){
	payload, err := json.Marshal(v)
	if err != nil{
		fmt.Println(fmt.Sprintf("error marshalling the request payload for command type %d", commandType))
		return []byte{}
	}

	req, err := http.NewRequest("POST", httpClient.url, bytes.NewBuffer(payload))
	// adding the commandType as a query param
	q := req.URL.Query()
	q.Add("commandType", commandType)
	req.URL.RawQuery = q.Encode()

	// panic in this case
	resp, err := httpClient.client.Do(req)
	if err != nil{
		fmt.Println(fmt.Sprintf("error sending the http request to url %s. Failed with error %s", httpClient.url, err.Error()))
		return []byte{}
	}
	// reading the entire response body and closing the response body.
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		fmt.Println("error reading response for the request")
		return []byte{}
	}
	// If the resp status is not accepted, then the request failed
	if resp.StatusCode != http.StatusAccepted{
		fmt.Println("request failed with status code %d and msg %s", resp.StatusCode, string(body))
		return []byte{}
	}
	return body
}

const DefaultBlockSize = 100 * 1024 * 1024
