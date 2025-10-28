// Copyright © 2025 Microsoft <wastore@microsoft.com>
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
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/JeffreyRichter/enum/enum"
)

type OutputFormat uint32

var EOutputFormat = OutputFormat(0)

func (OutputFormat) None() OutputFormat { return OutputFormat(0) }
func (OutputFormat) Text() OutputFormat { return OutputFormat(1) }
func (OutputFormat) Json() OutputFormat { return OutputFormat(2) }

func (of *OutputFormat) Parse(s string) error {
	val, err := enum.Parse(reflect.TypeOf(of), s, true)
	if err == nil {
		*of = val.(OutputFormat)
	}
	return err
}

func (of OutputFormat) String() string {
	return enum.StringInt(of, reflect.TypeOf(of))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EOutputVerbosity = OutputVerbosity(0)

type OutputVerbosity uint8

func (OutputVerbosity) Default() OutputVerbosity   { return OutputVerbosity(0) }
func (OutputVerbosity) Essential() OutputVerbosity { return OutputVerbosity(1) } // no progress, no info, no prompts. Print everything else
func (OutputVerbosity) Quiet() OutputVerbosity     { return OutputVerbosity(2) } // nothing at all

func (qm *OutputVerbosity) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(qm), s, true, true)
	if err == nil {
		*qm = val.(OutputVerbosity)
	}
	return err
}

func (qm OutputVerbosity) String() string {
	return enum.StringInt(qm, reflect.TypeOf(qm))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var EOutputMessageType = OutputMessageType(0)

// OutputMessageType defines the nature of the output, ex: progress report, job summary, or error
type OutputMessageType uint8

func (OutputMessageType) Init() OutputMessageType     { return OutputMessageType(0) } // simple print, allowed to float up
func (OutputMessageType) Info() OutputMessageType     { return OutputMessageType(1) } // simple print, allowed to float up
func (OutputMessageType) Progress() OutputMessageType { return OutputMessageType(2) } // should be printed on the same line over and over again, not allowed to float up
func (OutputMessageType) Dryrun() OutputMessageType   { return OutputMessageType(6) } // simple print

// EndOfJob used to be called Exit, but now it's not necessarily an exit, because we may have follow-up jobs
func (OutputMessageType) EndOfJob() OutputMessageType { return OutputMessageType(3) } // (may) exit after printing

func (OutputMessageType) Error() OutputMessageType  { return OutputMessageType(4) } // indicate fatal error, exit right after
func (OutputMessageType) Prompt() OutputMessageType { return OutputMessageType(5) } // ask the user a question after erasing the progress

func (OutputMessageType) Response() OutputMessageType { return OutputMessageType(7) } /* Response to LCMMsg (like PerformanceAdjustment)
//Json with determined fields for output-type json, INFO for other o/p types. */

func (OutputMessageType) ListObject() OutputMessageType  { return OutputMessageType(8) }
func (OutputMessageType) ListSummary() OutputMessageType { return OutputMessageType(9) }

func (OutputMessageType) LoginStatusInfo() OutputMessageType { return OutputMessageType(10) }

func (OutputMessageType) GetJobSummary() OutputMessageType    { return OutputMessageType(11) }
func (OutputMessageType) ListJobTransfers() OutputMessageType { return OutputMessageType(12) }

func (o OutputMessageType) String() string {
	return enum.StringInt(o, reflect.TypeOf(o))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// used for output types that are not simple strings, such as progress and init
// a given format(text,json) is passed in, and the appropriate string is returned
type OutputBuilder func(OutputFormat) string

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// note: if AzCopy exits due to a panic, we don't directly control what the exit code will be. The Go runtime seems to be
// hard-coded to give an exit code of 2 in that case, but there is discussion of changing it to 1, so it may become
// impossible to tell from exit code alone whether AzCopy panic or return EExitCode.Error.
// See https://groups.google.com/forum/#!topic/golang-nuts/u9NgKibJsKI
// However, fortunately, in the panic case, stderr will get the panic message;
// whereas AFAIK we never write to stderr in normal execution of AzCopy.  So that's a suggested way to differentiate when needed.

// TODO: if/when we review the STE structure, with regard to the old out-of-process design vs the current in-process design, we should
//   confirm whether we also need a separate exit code to signal process exit. For now, let's assume that anything listening to our stdout
//   will detect process exit (if needs to) by detecting that we have closed our stdout.

var EExitCode = ExitCode(0)

type ExitCode uint32

func (ExitCode) Success() ExitCode { return ExitCode(0) }
func (ExitCode) Error() ExitCode   { return ExitCode(1) }

// NoExit is used as a marker, to suppress the normal exit behaviour
func (ExitCode) NoExit() ExitCode { return ExitCode(99) }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// defines the output and how it should be handled
type outputMessage struct {
	msgContent    string
	msgType       OutputMessageType
	exitCode      ExitCode      // only for when the application is meant to exit after printing (i.e. Error or Final)
	inputChannel  chan<- string // support getting a response from the user
	promptDetails common.PromptDetails
}

func (m outputMessage) shouldExitProcess() bool {
	return m.msgType == EOutputMessageType.Error() ||
		(m.msgType == EOutputMessageType.EndOfJob() && !(m.exitCode == EExitCode.NoExit()))
}

// -------------------------------------- JSON templates -------------------------------------- //
// used to help formatting of JSON outputs

func GetJsonStringFromTemplate(template interface{}) string {
	jsonOutput, err := json.Marshal(template)
	common.PanicIfErr(err)

	return string(jsonOutput)
}

// defines the general output template when the format is set to json
type JsonOutputTemplate struct {
	TimeStamp      time.Time
	MessageType    string
	MessageContent string // a simple string for INFO and ERROR, a serialized JSON for INIT, PROGRESS, EXIT
	PromptDetails  common.PromptDetails
}

func newJsonOutputTemplate(messageType OutputMessageType, messageContent string, promptDetails common.PromptDetails) *JsonOutputTemplate {
	return &JsonOutputTemplate{TimeStamp: time.Now(), MessageType: messageType.String(),
		MessageContent: messageContent, PromptDetails: promptDetails}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type InitMsgJsonTemplate struct {
	LogFileLocation string
	JobID           string
	IsCleanupJob    bool
}

func GetStandardInitOutputBuilder(jobID string, logFileLocation string, isCleanupJob bool, cleanupMessage string) OutputBuilder {
	return func(format OutputFormat) string {
		if format == EOutputFormat.Json() {
			return GetJsonStringFromTemplate(InitMsgJsonTemplate{
				JobID:           jobID,
				LogFileLocation: logFileLocation,
				IsCleanupJob:    isCleanupJob,
			})
		}

		var sb strings.Builder
		if isCleanupJob {
			cleanupHeader := "(" + cleanupMessage + " with cleanup jobID " + jobID
			sb.WriteString(strings.Repeat("-", len(cleanupHeader)) + "\n")
			sb.WriteString(cleanupHeader)
		} else {
			sb.WriteString("\nJob " + jobID + " has started\n")
			if logFileLocation != "" {
				sb.WriteString("Log file is located at: " + logFileLocation)
			}
			sb.WriteString("\n")
		}
		return sb.String()
	}
}
