package common

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"

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
func (of OutputFormat) String() string { return enum.StringInt(of, reflect.TypeOf(of)) }

type OutputMessageType uint8

var EOutputMessageType = OutputMessageType(0)

func (OutputMessageType) Init() OutputMessageType             { return OutputMessageType(0) }
func (OutputMessageType) Info() OutputMessageType             { return OutputMessageType(1) }
func (OutputMessageType) Progress() OutputMessageType         { return OutputMessageType(2) }
func (OutputMessageType) EndOfJob() OutputMessageType         { return OutputMessageType(3) }
func (OutputMessageType) Error() OutputMessageType            { return OutputMessageType(4) }
func (OutputMessageType) Prompt() OutputMessageType           { return OutputMessageType(5) }
func (OutputMessageType) Dryrun() OutputMessageType           { return OutputMessageType(6) }
func (OutputMessageType) Response() OutputMessageType         { return OutputMessageType(7) }
func (OutputMessageType) ListObject() OutputMessageType       { return OutputMessageType(8) }
func (OutputMessageType) ListSummary() OutputMessageType      { return OutputMessageType(9) }
func (OutputMessageType) LoginStatusInfo() OutputMessageType  { return OutputMessageType(10) }
func (OutputMessageType) GetJobSummary() OutputMessageType    { return OutputMessageType(11) }
func (OutputMessageType) ListJobTransfers() OutputMessageType { return OutputMessageType(12) }
func (o OutputMessageType) String() string                    { return enum.StringInt(o, reflect.TypeOf(o)) }

type OutputBuilder func(OutputFormat) string
type ExitCode uint32

var EExitCode = ExitCode(0)

func (ExitCode) Success() ExitCode { return ExitCode(0) }
func (ExitCode) Error() ExitCode   { return ExitCode(1) }
func (ExitCode) NoExit() ExitCode  { return ExitCode(99) }

type JsonOutputTemplate struct {
	TimeStamp      time.Time
	MessageType    string
	MessageContent string
	PromptDetails  PromptDetails
}

func GetJsonStringFromTemplate(template interface{}) string {
	output, err := json.Marshal(template)
	PanicIfErr(err)
	return string(output)
}

type InitMsgJsonTemplate struct {
	LogFileLocation string
	JobID           string
	IsCleanupJob    bool
}

func GetStandardInitOutputBuilder(jobID string, logFileLocation string, isCleanupJob bool, cleanupMessage string) OutputBuilder {
	return func(format OutputFormat) string {
		if format == EOutputFormat.Json() {
			return GetJsonStringFromTemplate(InitMsgJsonTemplate{logFileLocation, jobID, isCleanupJob})
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
