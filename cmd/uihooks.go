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
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// hold a pointer to the global lifecycle controller so that commands could output messages and exit properly
// only one instance of the formatter should exist
var glcm LifecycleMgr = func() (lcmgr *lifecycleMgr) {
	lcmgr = &lifecycleMgr{
		msgQueue:             make(chan outputMessage, 1000),
		progressCache:        "",
		cancelChannel:        make(chan os.Signal, 1),
		e2eContinueChannel:   make(chan struct{}),
		e2eAllowOpenChannel:  make(chan struct{}),
		outputFormat:         EOutputFormat.Text(), // output text by default
		logSanitizer:         common.NewAzCopyLogSanitizer(),
		inputQueue:           make(chan userInput, 1000),
		allowCancelFromStdIn: false,
		allowWatchInput:      false,
		closeFunc:            func() {}, // noop since we have nothing to do by default
		waitForUserResponse:  make(chan bool),
		msgHandlerChannel:    make(chan *common.LCMMsg),
	}

	// kick off the single routine that processes output
	go lcmgr.processOutputMessage()

	// and process input
	go lcmgr.watchInputs()

	// Check if need to do CPU profiling, and do CPU profiling accordingly when azcopy life start.
	lcmgr.checkAndStartCPUProfiling()

	common.SetUIHooks(&common.JobUIHooks{
		Prompt:                 lcmgr.Prompt,
		Info:                   lcmgr.Info,
		Warn:                   lcmgr.Warn,
		E2EAwaitAllowOpenFiles: lcmgr.E2EAwaitAllowOpenFiles,
	})

	return
}()

func GetLifecycleMgr() LifecycleMgr {
	return glcm
}

// create a public interface so that consumers outside of this package can refer to the lifecycle manager
// but they would not be able to instantiate one
type LifecycleMgr interface {
	Init(OutputBuilder)                                                        // let the user know the job has started and initial information like log location
	Progress(OutputBuilder)                                                    // print on the same line over and over again, not allowed to float up
	Exit(OutputBuilder, ExitCode)                                              // indicates successful execution exit after printing, allow user to specify exit code
	Info(string)                                                               // simple print, allowed to float up
	Warn(string)                                                               // simple print, allowed to float up
	Dryrun(OutputBuilder)                                                      // print files for dry run mode
	Output(OutputBuilder, OutputMessageType)                                   // print output for list
	Error(string)                                                              // indicates fatal error, exit after printing, exit code is always Failed (1)
	Prompt(message string, details common.PromptDetails) common.ResponseOption // ask the user a question(after erasing the progress), then return the response
	SurrenderControl()                                                         // give up control, this should never return
	InitiateProgressReporting(WorkController)                                  // start writing progress with another routine
	AllowReinitiateProgressReporting()                                         // allow re-initiation of progress reporting for followup job
	SetOutputFormat(OutputFormat)                                              // change the output format of the entire application
	EnableInputWatcher()                                                       // depending on the command, we may allow user to give input through Stdin
	EnableCancelFromStdIn()                                                    // allow user to send in `cancel` to stop the job
	E2EAwaitContinue()                                                         // used by E2E tests
	E2EAwaitAllowOpenFiles()                                                   // used by E2E tests
	E2EEnableAwaitAllowOpenFiles(enable bool)                                  // used by E2E tests
	RegisterCloseFunc(func())
	MsgHandlerChannel() <-chan *common.LCMMsg
	ReportAllJobPartsDone()
	SetOutputVerbosity(mode OutputVerbosity)
	SetForceLogging()
}

// single point of control for all outputs
type lifecycleMgr struct {
	msgQueue              chan outputMessage
	progressCache         string // useful for keeping job progress on the last line
	cancelChannel         chan os.Signal
	doneChannel           chan bool
	e2eContinueChannel    chan struct{}
	e2eAllowOpenChannel   chan struct{}
	waitEverCalled        int32
	outputFormat          OutputFormat
	logSanitizer          common.LogSanitizer
	inputQueue            chan userInput // msgs from the user
	allowWatchInput       bool           // accept user inputs and place then in the inputQueue
	allowCancelFromStdIn  bool           // allow user to send in 'cancel' from the stdin to stop the current job
	e2eAllowAwaitContinue bool           // allow the user to send 'continue' from stdin to start the current job
	e2eAllowAwaitOpen     bool           // allow the user to send 'open' from stdin to allow the opening of the first file
	closeFunc             func()         // used to close logs before exiting
	waitForUserResponse   chan bool
	msgHandlerChannel     chan *common.LCMMsg
	OutputVerbosityType   OutputVerbosity
	disableSysLog         bool
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
		if err != nil {
			continue
		}

		// remove spaces before/after the content
		msg := strings.TrimSpace(input)
		timeReceived := time.Now()

		select {
		case <-lcm.waitForUserResponse:
			lcm.inputQueue <- userInput{timeReceived: timeReceived, content: msg}
			continue
		default:
		}

		allCharsAreWhiteSpace := true
		for _, ch := range msg {
			if !unicode.IsSpace(ch) {
				allCharsAreWhiteSpace = false
				break
			}
		}
		if allCharsAreWhiteSpace {
			continue
		}

		var req common.LCMMsgReq
		if lcm.allowCancelFromStdIn && strings.EqualFold(msg, "cancel") {
			lcm.cancelChannel <- os.Interrupt
		} else if lcm.e2eAllowAwaitContinue && strings.EqualFold(msg, "continue") {
			close(lcm.e2eContinueChannel)
		} else if lcm.e2eAllowAwaitOpen && strings.EqualFold(msg, "open") {
			close(lcm.e2eAllowOpenChannel)
		} else if err := json.Unmarshal([]byte(msg), &req); err == nil { //json string
			lcm.Info(fmt.Sprintf("Received request for %s with timeStamp %s", req.MsgType, req.TimeStamp.String()))
			var msgType common.LCMMsgType
			if err := msgType.Parse(req.MsgType); err != nil {
				lcm.Info(fmt.Sprintf("Discarding incorrect message: %s.", req.MsgType))
				continue
			}

			switch msgType {
			case common.ELCMMsgType.CancelJob():
				lcm.cancelChannel <- os.Interrupt
			default:
				m := common.NewLCMMsg()
				m.Req = &req
				lcm.msgHandlerChannel <- m

				//wait till the message is completed
				<-m.RespChan
				lcm.Response(*m.Resp)
			}
		} else {
			lcm.Info("Discarding incorrectly formatted input message")
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

func (lcm *lifecycleMgr) SetOutputFormat(format OutputFormat) {
	lcm.outputFormat = format
}

func (lcm *lifecycleMgr) checkAndStartCPUProfiling() {
	// CPU Profiling add-on. Set AZCOPY_PROFILE_CPU to enable CPU profiling,
	// the value AZCOPY_PROFILE_CPU indicates the path to save CPU profiling data.
	// e.g. export AZCOPY_PROFILE_CPU="cpu.prof"
	// For more details, please refer to https://golang.org/pkg/runtime/pprof/
	cpuProfilePath := common.GetEnvironmentVariable(common.EEnvironmentVariable.ProfileCPU())
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
	memProfilePath := common.GetEnvironmentVariable(common.EEnvironmentVariable.ProfileMemory())
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
		msgType:    EOutputMessageType.Init(),
	}
}

func (lcm *lifecycleMgr) Progress(o OutputBuilder) {
	messageContent := ""
	if o != nil {
		messageContent = o(lcm.outputFormat)
	}

	lcm.msgQueue <- outputMessage{
		msgContent: messageContent,
		msgType:    EOutputMessageType.Progress(),
	}
}

func (lcm *lifecycleMgr) Info(msg string) {

	msg = lcm.logSanitizer.SanitizeLogMessage(msg) // sometimes error-like text comes through Info, before the final "we've failed, please stop now" signal comes to Error. So we sanitize in both places.

	infoMsg := fmt.Sprintf("INFO: %v", msg)

	lcm.msgQueue <- outputMessage{
		msgContent: infoMsg,
		msgType:    EOutputMessageType.Info(),
	}
}

func (lcm *lifecycleMgr) Warn(msg string) {

	msg = lcm.logSanitizer.SanitizeLogMessage(msg) // sometimes error-like text comes through Info, before the final "we've failed, please stop now" signal comes to Error. So we sanitize in both places.

	infoMsg := fmt.Sprintf("WARN: %v", msg)

	lcm.msgQueue <- outputMessage{
		msgContent: infoMsg,
		msgType:    EOutputMessageType.Info(),
	}
}

func (lcm *lifecycleMgr) Prompt(message string, details common.PromptDetails) common.ResponseOption {

	expectedInputChannel := make(chan string, 1)
	lcm.msgQueue <- outputMessage{
		msgContent:    message,
		msgType:       EOutputMessageType.Prompt(),
		inputChannel:  expectedInputChannel,
		promptDetails: details,
	}

	// Request watchInputs() to wait for response from user
	lcm.waitForUserResponse <- true

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
	return common.EResponseOption.Default()
}

func (lcm *lifecycleMgr) Dryrun(o OutputBuilder) {
	dryrunMessage := ""
	if o != nil {
		dryrunMessage = o(lcm.outputFormat)
	}

	lcm.msgQueue <- outputMessage{
		msgContent: dryrunMessage,
		msgType:    EOutputMessageType.Dryrun(),
	}
}

func (lcm *lifecycleMgr) Output(o OutputBuilder, msgType OutputMessageType) {
	om := ""
	if o != nil {
		om = o(lcm.outputFormat)
	}

	lcm.msgQueue <- outputMessage{
		msgContent: om,
		msgType:    msgType,
	}
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
		msgType:    EOutputMessageType.Error(),
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
		msgType:    EOutputMessageType.EndOfJob(),
		exitCode:   applicationExitCode,
	}

	if common.AzcopyCurrentJobLogger != nil && applicationExitCode != EExitCode.NoExit() {
		common.AzcopyCurrentJobLogger.CloseLog()
	}

	if applicationExitCode != EExitCode.NoExit() {
		// stall forever until the success message is printed and program exits
		lcm.SurrenderControl()
	}
}

func (lcm *lifecycleMgr) Response(resp common.LCMMsgResp) {

	var respMsg string

	if lcm.outputFormat == EOutputFormat.Json() {
		m, err := json.Marshal(resp)
		respMsg = string(m)
		common.PanicIfErr(err)
	} else {
		respMsg = fmt.Sprintf("INFO: %v", resp.Value.String())
	}

	respMsg = lcm.logSanitizer.SanitizeLogMessage(respMsg)

	lcm.msgQueue <- outputMessage{
		msgContent: respMsg,
		msgType:    EOutputMessageType.Response(),
	}
}

// this is used by commands that wish to stall forever to wait for the operations to complete
func (lcm *lifecycleMgr) SurrenderControl() {
	// stall forever
	select {}
}

func (lcm *lifecycleMgr) RegisterCloseFunc(closeFunc func()) {
	if lcm.closeFunc != nil {
		// "dereference" the function for later calling
		orig := lcm.closeFunc
		lcm.closeFunc = func() {
			orig()
			closeFunc()
		}
	} else {
		lcm.closeFunc = closeFunc
	}
}

func (lcm *lifecycleMgr) processOutputMessage() {
	// this function constantly pulls out message to output
	// and pass them onto the right handler based on the output format
	for {
		msgToPrint := <-lcm.msgQueue

		if shouldQuietMessage(msgToPrint, lcm.OutputVerbosityType) {
			lcm.processNoneOutput(msgToPrint)
			continue
		}
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
	if msgToOutput.msgType == EOutputMessageType.Error() {
		lcm.closeFunc()
		os.Exit(int(EExitCode.Error()))
	} else if msgToOutput.shouldExitProcess() {
		lcm.closeFunc()
		os.Exit(int(msgToOutput.exitCode))
	}
	// ignore all other outputs
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
		lcm.closeFunc()
		os.Exit(int(msgToOutput.exitCode))
	} else if msgType == EOutputMessageType.Prompt() {
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
	case EOutputMessageType.Error(), EOutputMessageType.EndOfJob():
		// simply print and quit
		// if no message is intended, avoid adding new lines
		if msgToOutput.msgContent != "" {
			fmt.Println("\n" + msgToOutput.msgContent)
		}
		if msgToOutput.shouldExitProcess() {
			lcm.closeFunc()
			os.Exit(int(msgToOutput.exitCode))
		}

	case EOutputMessageType.Progress():
		fmt.Print("\r")                   // return carriage back to start
		fmt.Print(msgToOutput.msgContent) // print new progress

		// it is possible that the new progress status is somehow shorter than the previous one
		// in this case we must erase the left over characters from the previous progress
		matchLengthWithSpaces(len(lcm.progressCache), len(msgToOutput.msgContent))

		lcm.progressCache = msgToOutput.msgContent
	case EOutputMessageType.Prompt():
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
	default:
		// Init, Info, Dryrun, Response, ListSummary, ListObject, and any other new message types will use default
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
	}
}

// for the lifecycleMgr to babysit a job, it must be given a controller to get information about the job
type WorkController interface {
	Cancel(mgr LifecycleMgr)                                        // handle to cancel the work
	ReportProgressOrExit(mgr LifecycleMgr) (totalKnownCount uint32) // print the progress status, optionally exit the application if work is done
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
		const progressFrequencyThreshold = 1000000
		var oldCount, newCount uint32
		wait := 2 * time.Second
		lastFetchTime := time.Now().Add(-wait) // So that we start fetching time immediately

		// cancelChannel will be notified when os receives os.Interrupt and os.Kill signals
		signal.Notify(lcm.cancelChannel, os.Interrupt, syscall.SIGTERM)

		cancelCalled := false

		doCancel := func() {
			cancelCalled = true
			lcm.Info("Cancellation requested. Beginning clean shutdown...")
			jc.Cancel(lcm)
		}

		for {
			select {
			case <-lcm.cancelChannel:
				doCancel()
				continue // to exit on next pass through loop
			case <-lcm.doneChannel:

				newCount = jc.ReportProgressOrExit(lcm)
				lastFetchTime = time.Now()
			case <-time.After(wait):
				if time.Since(lastFetchTime) >= wait {
					newCount = jc.ReportProgressOrExit(lcm)
					lastFetchTime = time.Now()
				}
			}

			if newCount >= progressFrequencyThreshold && !cancelCalled {
				// report less on progress  - to save on the CPU costs of doing so and because, if there are this many files,
				// its going to be a long job anyway, so no need to report so often
				wait = 2 * time.Minute
				if oldCount < progressFrequencyThreshold {
					lcm.Info(fmt.Sprintf("Reducing progress output frequency to %v, because there are over %d files", wait, progressFrequencyThreshold))
				}
			}

			oldCount = newCount
		}
	}()
}

func (_ *lifecycleMgr) awaitChannel(ch chan struct{}, timeout time.Duration) {
	select {
	case <-ch:
	case <-time.After(timeout):
	}
}

// E2EAwaitContinue is used in case where a developer wants to debug AzCopy by attaching to the running process,
// before it starts doing any actual work.
func (lcm *lifecycleMgr) E2EAwaitContinue() {
	lcm.e2eAllowAwaitContinue = true // not technically gorountine safe (since its shared state) but its consistent with EnableInputWatcher
	lcm.EnableInputWatcher()
	lcm.awaitChannel(lcm.e2eContinueChannel, time.Minute)
}

// E2EAwaitAllowOpenFiles is used in cases where we want to artificially produce a pause between enumeration and sending
// of the first file, for test purposes. (It only achieves that effect when the total file count is <= size of one job part).
// Does not pause at all, unless the feature has been enabled with a command-line flag.
func (lcm *lifecycleMgr) E2EAwaitAllowOpenFiles() {
	lcm.awaitChannel(lcm.e2eAllowOpenChannel, 5*time.Minute)
}

func (lcm *lifecycleMgr) E2EEnableAwaitAllowOpenFiles(enable bool) {
	if enable {
		lcm.e2eAllowAwaitOpen = true // not technically gorountine safe (since its shared state) but its consistent with EnableInputWatcher
		lcm.EnableInputWatcher()
	} else {
		close(lcm.e2eAllowOpenChannel) // so that E2EAwaitAllowOpenFiles will instantly return every time
	}
}

func (lcm *lifecycleMgr) MsgHandlerChannel() <-chan *common.LCMMsg {
	return lcm.msgHandlerChannel

}

func (lcm *lifecycleMgr) ReportAllJobPartsDone() {
	lcm.doneChannel <- true
}

func (lcm *lifecycleMgr) SetOutputVerbosity(mode OutputVerbosity) {
	lcm.OutputVerbosityType = mode
}

func (lcm *lifecycleMgr) SetForceLogging() {
	disableSyslog, err := strconv.ParseBool(common.GetEnvironmentVariable(common.EEnvironmentVariable.DisableSyslog()))
	if err != nil {
		// By default, we'll retain the current behaviour. i.e. To log in Syslog/WindowsEventLog if not specified by the user
		disableSyslog = false
	}
	lcm.disableSysLog = disableSyslog
}

func shouldQuietMessage(msgToOutput outputMessage, quietMode OutputVerbosity) bool {
	messageType := msgToOutput.msgType

	switch quietMode {
	case EOutputVerbosity.Default():
		return false
	case EOutputVerbosity.Essential():
		return messageType == EOutputMessageType.Progress() || messageType == EOutputMessageType.Info() || messageType == EOutputMessageType.Prompt()
	case EOutputVerbosity.Quiet():
		return true
	default:
		return false
	}
}
