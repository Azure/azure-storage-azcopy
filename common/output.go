package common

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/JeffreyRichter/enum/enum"
)

var EOutputMessageType = OutputMessageType(0)

// OutputMessageType defines the nature of the output, ex: progress report, job summary, or error
type OutputMessageType uint8

func (OutputMessageType) Init() OutputMessageType     { return OutputMessageType(0) } // simple print, allowed to float up
func (OutputMessageType) Info() OutputMessageType     { return OutputMessageType(1) } // simple print, allowed to float up
func (OutputMessageType) Progress() OutputMessageType { return OutputMessageType(2) } // should be printed on the same line over and over again, not allowed to float up
func (OutputMessageType) Dryrun() OutputMessageType   { return OutputMessageType(6) } // simple print

// EndOfJob used to be called Exit, but now it's not necessarily an exit, because we may have follow-up jobs
func (OutputMessageType) EndOfJob() OutputMessageType { return OutputMessageType(3) } // (may) exit after printing
// TODO: if/when we review the STE structure, with regard to the old out-of-process design vs the current in-process design, we should
//   confirm whether we also need a separate exit code to signal process exit. For now, let's assume that anything listening to our stdout
//   will detect process exit (if needs to) by detecting that we have closed our stdout.

func (OutputMessageType) Error() OutputMessageType  { return OutputMessageType(4) } // indicate fatal error, exit right after
func (OutputMessageType) Prompt() OutputMessageType { return OutputMessageType(5) } // ask the user a question after erasing the progress

func (OutputMessageType) Response() OutputMessageType { return OutputMessageType(7) } /* Response to LCMMsg (like PerformanceAdjustment)
//Json with determined fields for output-type json, INFO for other o/p types. */

// ListOutputTypes

func (OutputMessageType) ListObject() OutputMessageType  { return OutputMessageType(8) }
func (OutputMessageType) ListSummary() OutputMessageType { return OutputMessageType(9) }

func (OutputMessageType) LoginStatusInfo() OutputMessageType { return OutputMessageType(10) }

func (OutputMessageType) GetJobSummary() OutputMessageType    { return OutputMessageType(11) }
func (OutputMessageType) ListJobTransfers() OutputMessageType { return OutputMessageType(12) }

func (o OutputMessageType) String() string {
	return enum.StringInt(o, reflect.TypeOf(o))
}

// defines the output and how it should be handled
type outputMessage struct {
	msgContent    string
	msgType       OutputMessageType
	exitCode      ExitCode      // only for when the application is meant to exit after printing (i.e. Error or Final)
	inputChannel  chan<- string // support getting a response from the user
	promptDetails PromptDetails
}

func (m outputMessage) shouldExitProcess() bool {
	return m.msgType == EOutputMessageType.Error() ||
		(m.msgType == EOutputMessageType.EndOfJob() && !(m.exitCode == EExitCode.NoExit()))
}

// used for output types that are not simple strings, such as progress and init
// a given format(text,json) is passed in, and the appropriate string is returned
type OutputBuilder func(OutputFormat) string

type PromptDetails struct {
	PromptType      PromptType
	ResponseOptions []ResponseOption // used from prompt messages where we expect a response
	PromptTarget    string           // used when prompt message is targeting a specific resource, ease partner team integration
}

var EPromptType = PromptType("")

type PromptType string

func (PromptType) Reauth() PromptType            { return PromptType("Reauth") }
func (PromptType) Cancel() PromptType            { return PromptType("Cancel") }
func (PromptType) Overwrite() PromptType         { return PromptType("Overwrite") }
func (PromptType) DeleteDestination() PromptType { return PromptType("DeleteDestination") }

// -------------------------------------- JSON templates -------------------------------------- //
// used to help formatting of JSON outputs

func GetJsonStringFromTemplate(template interface{}) string {
	jsonOutput, err := json.Marshal(template)
	PanicIfErr(err)

	return string(jsonOutput)
}

// defines the general output template when the format is set to json
type JsonOutputTemplate struct {
	TimeStamp      time.Time
	MessageType    string
	MessageContent string // a simple string for INFO and ERROR, a serialized JSON for INIT, PROGRESS, EXIT
	PromptDetails  PromptDetails
}

func newJsonOutputTemplate(messageType OutputMessageType, messageContent string, promptDetails PromptDetails) *JsonOutputTemplate {
	return &JsonOutputTemplate{TimeStamp: time.Now(), MessageType: messageType.String(),
		MessageContent: messageContent, PromptDetails: promptDetails}
}

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
