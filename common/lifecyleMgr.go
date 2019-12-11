package common

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

// only one instance of the formatter should exist
var lcm = func() (lcmgr *lifecycleMgr) {
	lcmgr = &lifecycleMgr{
		msgQueue:             make(chan outputMessage, 1000),
		progressCache:        "",
		cancelChannel:        make(chan os.Signal, 1),
		outputFormat:         EOutputFormat.Text(), // output text by default
		logSanitizer:         NewAzCopyLogSanitizer(),
		inputQueue:           make(chan userInput, 1000),
		allowCancelFromStdIn: false,
		allowWatchInput:      false,
	}

	// kick off the single routine that processes output
	go lcmgr.processOutputMessage()

	// and process input
	go lcmgr.watchInputs()

	// Check if need to do CPU profiling, and do CPU profiling accordingly when azcopy life start.
	lcmgr.checkAndStartCPUProfiling()

	return
}()

// create a public interface so that consumers outside of this package can refer to the lifecycle manager
// but they would not be able to instantiate one
type LifecycleMgr interface {
	Init(OutputBuilder)                                          // let the user know the job has started and initial information like log location
	Progress(OutputBuilder)                                      // print on the same line over and over again, not allowed to float up
	Exit(OutputBuilder, ExitCode)                                // indicates successful execution exit after printing, allow user to specify exit code
	Info(string)                                                 // simple print, allowed to float up
	Error(string)                                                // indicates fatal error, exit after printing, exit code is always Failed (1)
	Prompt(message string, details PromptDetails) ResponseOption // ask the user a question(after erasing the progress), then return the response
	SurrenderControl()                                           // give up control, this should never return
	InitiateProgressReporting(WorkController)                    // start writing progress with another routine
	AllowReinitiateProgressReporting()                           // allow re-initiation of progress reporting for followup job
	GetEnvironmentVariable(EnvironmentVariable) string           // get the environment variable or its default value
	ClearEnvironmentVariable(EnvironmentVariable)                // clears the environment variable
	SetOutputFormat(OutputFormat)                                // change the output format of the entire application
	EnableInputWatcher()                                         // depending on the command, we may allow user to give input through Stdin
	EnableCancelFromStdIn()                                      // allow user to send in `cancel` to stop the job
	AddUserAgentPrefix(string) string                            // append the global user agent prefix, if applicable
}

func GetLifecycleMgr() LifecycleMgr {
	return lcm
}

// single point of control for all outputs
type lifecycleMgr struct {
	msgQueue             chan outputMessage
	progressCache        string // useful for keeping job progress on the last line
	cancelChannel        chan os.Signal
	waitEverCalled       int32
	outputFormat         OutputFormat
	logSanitizer         pipeline.LogSanitizer
	inputQueue           chan userInput // msgs from the user
	allowWatchInput      bool           // accept user inputs and place then in the inputQueue
	allowCancelFromStdIn bool           // allow user to send in 'cancel' from the stdin to stop the current job
}

type userInput struct {
	timeReceived time.Time
	content      string
}

// should be started in a single go routine
func (lcm *lifecycleMgr) watchInputs() {
	consoleReader := bufio.NewReader(os.Stdin)
	for {
		// sleep for a bit, the option might be enabled later
		if !lcm.allowWatchInput {
			time.Sleep(time.Microsecond * 500)
			continue
		}

		// reads input until the first occurrence of \n in the input,
		input, err := consoleReader.ReadString('\n')
		timeReceived := time.Now()
		if err != nil {
			continue
		}

		// remove spaces before/after the content
		msg := strings.TrimSpace(input)

		if lcm.allowCancelFromStdIn && strings.EqualFold(msg, "cancel") {
			lcm.cancelChannel <- os.Interrupt
		} else {
			lcm.inputQueue <- userInput{timeReceived: timeReceived, content: msg}
		}
	}
}

// get the answer to a question that was asked at a certain time
// only user input after the specified time is returned to make sure that we are getting the right answer to our question
// NOTE: to ask a question, go through Prompt, to guarantee that only 1 question is asked at a time
func (lcm *lifecycleMgr) getInputAfterTime(time time.Time) string {
	for {
		msg := <-lcm.inputQueue

		// keep reading until we find an input that came in after the user specified time
		if msg.timeReceived.After(time) {
			return msg.content
		}

		// otherwise keep waiting as it's possible that the user has not typed it in yet
	}
}

func (lcm *lifecycleMgr) EnableInputWatcher() {
	lcm.allowWatchInput = true
}

func (lcm *lifecycleMgr) EnableCancelFromStdIn() {
	lcm.allowCancelFromStdIn = true
}

func (lcm *lifecycleMgr) ClearEnvironmentVariable(variable EnvironmentVariable) {
	_ = os.Setenv(variable.Name, "")
}

func (lcm *lifecycleMgr) SetOutputFormat(format OutputFormat) {
	lcm.outputFormat = format
}

func (lcm *lifecycleMgr) checkAndStartCPUProfiling() {
	// CPU Profiling add-on. Set AZCOPY_PROFILE_CPU to enable CPU profiling,
	// the value AZCOPY_PROFILE_CPU indicates the path to save CPU profiling data.
	// e.g. export AZCOPY_PROFILE_CPU="cpu.prof"
	// For more details, please refer to https://golang.org/pkg/runtime/pprof/
	cpuProfilePath := lcm.GetEnvironmentVariable(EEnvironmentVariable.ProfileCPU())
	if cpuProfilePath != "" {
		lcm.Info(fmt.Sprintf("pprof start CPU profiling, and saving profiling data to: %q", cpuProfilePath))
		f, err := os.Create(cpuProfilePath)
		if err != nil {
			lcm.Error(fmt.Sprintf("Fail to create file for CPU profiling, %v", err))
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			lcm.Error(fmt.Sprintf("Fail to start CPU profiling, %v", err))
		}
	}
}

func (lcm *lifecycleMgr) checkAndStopCPUProfiling() {
	// Stop CPU profiling if there is ongoing CPU profiling.
	pprof.StopCPUProfile()
}

func (lcm *lifecycleMgr) checkAndTriggerMemoryProfiling() {
	// Memory Profiling add-on. Set AZCOPY_PROFILE_MEM to enable memory profiling,
	// the value AZCOPY_PROFILE_MEM indicates the path to save memory profiling data.
	// e.g. export AZCOPY_PROFILE_MEM="mem.prof"
	// For more details, please refer to https://golang.org/pkg/runtime/pprof/
	memProfilePath := lcm.GetEnvironmentVariable(EEnvironmentVariable.ProfileMemory())
	if memProfilePath != "" {
		lcm.Info(fmt.Sprintf("pprof start memory profiling, and saving profiling data to: %q", memProfilePath))
		f, err := os.Create(memProfilePath)
		if err != nil {
			lcm.Error(fmt.Sprintf("Fail to create file for memory profiling, %v", err))
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			lcm.Error(fmt.Sprintf("Fail to start memory profiling, %v", err))
		}
		if err := f.Close(); err != nil {
			lcm.Info(fmt.Sprintf("Fail to close memory profiling file, %v", err))
		}
	}
}

func (lcm *lifecycleMgr) Init(o OutputBuilder) {
	lcm.msgQueue <- outputMessage{
		msgContent: o(lcm.outputFormat),
		msgType:    eOutputMessageType.Init(),
	}
}

func (lcm *lifecycleMgr) Progress(o OutputBuilder) {
	messageContent := ""
	if o != nil {
		messageContent = o(lcm.outputFormat)
	}

	lcm.msgQueue <- outputMessage{
		msgContent: messageContent,
		msgType:    eOutputMessageType.Progress(),
	}
}

func (lcm *lifecycleMgr) Info(msg string) {

	msg = lcm.logSanitizer.SanitizeLogMessage(msg) // sometimes error-like text comes through Info, before the final "we've failed, please stop now" signal comes to Error. So we sanitize in both places.

	infoMsg := fmt.Sprintf("INFO: %v", msg)

	lcm.msgQueue <- outputMessage{
		msgContent: infoMsg,
		msgType:    eOutputMessageType.Info(),
	}
}

func (lcm *lifecycleMgr) Prompt(message string, details PromptDetails) ResponseOption {
	expectedInputChannel := make(chan string, 1)
	lcm.msgQueue <- outputMessage{
		msgContent:    message,
		msgType:       eOutputMessageType.Prompt(),
		inputChannel:  expectedInputChannel,
		promptDetails: details,
	}

	// block until input comes from the user
	rawResponse := <-expectedInputChannel

	// match the given response against one of the options we gave
	for _, option := range details.ResponseOptions {
		// in case the user misunderstood and typed full response type instead, we still tolerate it
		// e.g. instead of "y", user typed "Yes"
		if strings.EqualFold(option.ResponseString, rawResponse) ||
			strings.EqualFold(option.UserFriendlyResponseType, rawResponse) {
			return option
		}
	}

	// nothing matched our options, assume default behavior (up to whoever that called Prompt)
	// we don't re-prompt the user since this makes the integration with Stg Exp more complex
	return EResponseOption.Default()
}

// TODO minor: consider merging with Exit
func (lcm *lifecycleMgr) Error(msg string) {

	msg = lcm.logSanitizer.SanitizeLogMessage(msg)

	// Check if need to do memory profiling, and do memory profiling accordingly before azcopy exits.
	lcm.checkAndTriggerMemoryProfiling()

	// Check if there is ongoing CPU profiling, and stop CPU profiling.
	lcm.checkAndStopCPUProfiling()

	lcm.msgQueue <- outputMessage{
		msgContent: msg,
		msgType:    eOutputMessageType.Error(),
		exitCode:   EExitCode.Error(),
	}

	// stall forever until the success message is printed and program exits
	lcm.SurrenderControl()
}

func (lcm *lifecycleMgr) Exit(o OutputBuilder, applicationExitCode ExitCode) {
	if applicationExitCode != EExitCode.NoExit() {
		// Check if need to do memory profiling, and do memory profiling accordingly before azcopy exits.
		lcm.checkAndTriggerMemoryProfiling()

		// Check if there is ongoing CPU profiling, and stop CPU profiling.
		lcm.checkAndStopCPUProfiling()
	}

	messageContent := ""
	if o != nil {
		messageContent = o(lcm.outputFormat)
	}

	lcm.msgQueue <- outputMessage{
		msgContent: messageContent,
		msgType:    eOutputMessageType.EndOfJob(),
		exitCode:   applicationExitCode,
	}

	if applicationExitCode != EExitCode.NoExit() {
		// stall forever until the success message is printed and program exits
		lcm.SurrenderControl()
	}
}

// this is used by commands that wish to stall forever to wait for the operations to complete
func (lcm *lifecycleMgr) SurrenderControl() {
	// stall forever
	select {}
}

func (lcm *lifecycleMgr) processOutputMessage() {
	// this function constantly pulls out message to output
	// and pass them onto the right handler based on the output format
	for {
		msgToPrint := <-lcm.msgQueue

		switch lcm.outputFormat {
		case EOutputFormat.Json():
			lcm.processJSONOutput(msgToPrint)
		case EOutputFormat.Text():
			lcm.processTextOutput(msgToPrint)
		case EOutputFormat.None():
			lcm.processNoneOutput(msgToPrint)
		default:
			panic("unimplemented output format")
		}
	}
}

func (lcm *lifecycleMgr) processNoneOutput(msgToOutput outputMessage) {
	if msgToOutput.msgType == eOutputMessageType.Error() {
		os.Exit(int(EExitCode.Error()))
	} else if msgToOutput.shouldExitProcess() {
		os.Exit(int(msgToOutput.exitCode))
	}

	// ignore all other outputs
	return
}

func (lcm *lifecycleMgr) processJSONOutput(msgToOutput outputMessage) {
	msgType := msgToOutput.msgType
	questionTime := time.Now()

	// simply output the json message
	// we assume the msgContent is already formatted correctly
	fmt.Println(GetJsonStringFromTemplate(newJsonOutputTemplate(msgType, msgToOutput.msgContent,
		msgToOutput.promptDetails)))

	// exit if needed
	if msgToOutput.shouldExitProcess() {
		os.Exit(int(msgToOutput.exitCode))
	} else if msgType == eOutputMessageType.Prompt() {
		// read the response to the prompt and send it back through the channel
		msgToOutput.inputChannel <- lcm.getInputAfterTime(questionTime)
	}
}

func (lcm *lifecycleMgr) processTextOutput(msgToOutput outputMessage) {
	// when a new line needs to overwrite the current line completely
	// we need to make sure that if the new line is shorter, we properly erase everything from the current line
	var matchLengthWithSpaces = func(curLineLength, newLineLength int) {
		if dirtyLeftover := curLineLength - newLineLength; dirtyLeftover > 0 {
			for i := 0; i < dirtyLeftover; i++ {
				fmt.Print(" ")
			}
		}
	}

	switch msgToOutput.msgType {
	case eOutputMessageType.Error(), eOutputMessageType.EndOfJob():
		// simply print and quit
		// if no message is intended, avoid adding new lines
		if msgToOutput.msgContent != "" {
			fmt.Println("\n" + msgToOutput.msgContent)
		}
		if msgToOutput.shouldExitProcess() {
			os.Exit(int(msgToOutput.exitCode))
		}

	case eOutputMessageType.Progress():
		fmt.Print("\r")                   // return carriage back to start
		fmt.Print(msgToOutput.msgContent) // print new progress

		// it is possible that the new progress status is somehow shorter than the previous one
		// in this case we must erase the left over characters from the previous progress
		matchLengthWithSpaces(len(lcm.progressCache), len(msgToOutput.msgContent))

		lcm.progressCache = msgToOutput.msgContent

	case eOutputMessageType.Init(), eOutputMessageType.Info():
		if lcm.progressCache != "" { // a progress status is already on the last line
			// print the info from the beginning on current line
			fmt.Print("\r")
			fmt.Print(msgToOutput.msgContent)

			// it is possible that the info is shorter than the progress status
			// in this case we must erase the left over characters from the progress status
			matchLengthWithSpaces(len(lcm.progressCache), len(msgToOutput.msgContent))

			// print the previous progress status again, so that it's on the last line
			fmt.Print("\n")
			fmt.Print(lcm.progressCache)
		} else {
			fmt.Println(msgToOutput.msgContent)
		}
	case eOutputMessageType.Prompt():
		questionTime := time.Now()

		if lcm.progressCache != "" { // a progress status is already on the last line
			// print the prompt from the beginning on current line
			fmt.Print("\r")
			fmt.Print(msgToOutput.msgContent)

			// it is possible that the prompt is shorter than the progress status
			// in this case we must erase the left over characters from the progress status
			matchLengthWithSpaces(len(lcm.progressCache), len(msgToOutput.msgContent))

		} else {
			fmt.Print(msgToOutput.msgContent)
		}

		// example output: Please confirm with: [Y] Yes  [N] No  [A] Yes for all  [L] No for all
		fmt.Print(" Please confirm with:")
		for _, option := range msgToOutput.promptDetails.ResponseOptions {
			fmt.Printf(" [%s] %s ", strings.ToUpper(option.ResponseString), option.UserFriendlyResponseType)
		}

		// read the response to the prompt and send it back through the channel
		msgToOutput.inputChannel <- lcm.getInputAfterTime(questionTime)
	}
}

// for the lifecycleMgr to babysit a job, it must be given a controller to get information about the job
type WorkController interface {
	Cancel(mgr LifecycleMgr)               // handle to cancel the work
	ReportProgressOrExit(mgr LifecycleMgr) // print the progress status, optionally exit the application if work is done
}

// AllowReinitiateProgressReporting must be called before running an cleanup job, to allow the initiation of that job's
// progress reporting to begin
func (lcm *lifecycleMgr) AllowReinitiateProgressReporting() {
	atomic.StoreInt32(&lcm.waitEverCalled, 0)
}

// isInteractive indicates whether the application was spawned by an actual user on the command
func (lcm *lifecycleMgr) InitiateProgressReporting(jc WorkController) {
	if !atomic.CompareAndSwapInt32(&lcm.waitEverCalled, 0, 1) {
		return
	}

	// this go routine never returns
	// it will terminate the whole process eventually when the work is complete
	go func() {
		// cancelChannel will be notified when os receives os.Interrupt and os.Kill signals
		signal.Notify(lcm.cancelChannel, os.Interrupt, os.Kill)

		for {
			select {
			case <-lcm.cancelChannel:
				lcm.Info("Cancellation requested. Beginning clean shutdown...")
				jc.Cancel(lcm)
			default:
				jc.ReportProgressOrExit(lcm)
			}

			// wait a bit before fetching job status again, as fetching has costs associated with it on the backend
			time.Sleep(2 * time.Second)
		}
	}()
}

func (lcm *lifecycleMgr) GetEnvironmentVariable(env EnvironmentVariable) string {
	value := os.Getenv(env.Name)
	if value == "" {
		return env.DefaultValue
	}
	return value
}

func (lcm *lifecycleMgr) AddUserAgentPrefix(userAgent string) string {
	prefix := lcm.GetEnvironmentVariable(EEnvironmentVariable.UserAgentPrefix())
	if len(prefix) > 0 {
		userAgent = prefix + " " + userAgent
	}

	return userAgent
}

// captures the common logic of exiting if there's an expected error
func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
