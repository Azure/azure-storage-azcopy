package common

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"
)

// only one instance of the formatter should exist
var lcm = func() (lcmgr *lifecycleMgr) {
	lcmgr = &lifecycleMgr{
		msgQueue:      make(chan outputMessage, 1000),
		progressCache: "",
		cancelChannel: make(chan os.Signal, 1),
		outputFormat:  EOutputFormat.Text(), // output text by default
	}

	// kick off the single routine that processes output
	go lcmgr.processOutputMessage()

	// Check if need to do CPU profiling, and do CPU profiling accordingly when azcopy life start.
	lcmgr.checkAndStartCPUProfiling()

	return
}()

// create a public interface so that consumers outside of this package can refer to the lifecycle manager
// but they would not be able to instantiate one
type LifecycleMgr interface {
	Init(OutputBuilder)                                // let the user know the job has started and initial information like log location
	Progress(OutputBuilder)                            // print on the same line over and over again, not allowed to float up
	Exit(OutputBuilder, ExitCode)                      // indicates successful execution exit after printing, allow user to specify exit code
	Info(string)                                       // simple print, allowed to float up
	Error(string)                                      // indicates fatal error, exit after printing, exit code is always Failed (1)
	Prompt(string) string                              // ask the user a question(after erasing the progress), then return the response
	SurrenderControl()                                 // give up control, this should never return
	InitiateProgressReporting(WorkController, bool)    // start writing progress with another routine
	GetEnvironmentVariable(EnvironmentVariable) string // get the environment variable or its default value
	SetOutputFormat(OutputFormat)                      // change the output format of the entire application
}

func GetLifecycleMgr() LifecycleMgr {
	return lcm
}

// single point of control for all outputs
type lifecycleMgr struct {
	msgQueue       chan outputMessage
	progressCache  string // useful for keeping job progress on the last line
	cancelChannel  chan os.Signal
	waitEverCalled int32
	outputFormat   OutputFormat
}

func (lcm *lifecycleMgr) SetOutputFormat(format OutputFormat) {
	lcm.outputFormat = format
}

func (lcm *lifecycleMgr) checkAndStartCPUProfiling() {
	// CPU Profiling add-on. Set AZCOPY_PROFILE_CPU to enable CPU profiling,
	// the value AZCOPY_PROFILE_CPU indicates the path to save CPU profiling data.
	// e.g. export AZCOPY_PROFILE_CPU="cpu.prof"
	// For more details, please refer to https://golang.org/pkg/runtime/pprof/
	cpuProfilePath := os.Getenv("AZCOPY_PROFILE_CPU")
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
	memProfilePath := os.Getenv("AZCOPY_PROFILE_MEM")
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
	lcm.msgQueue <- outputMessage{
		msgContent: msg,
		msgType:    eOutputMessageType.Info(),
	}
}

func (lcm *lifecycleMgr) Prompt(msg string) string {
	expectedInputChannel := make(chan string, 1)
	lcm.msgQueue <- outputMessage{
		msgContent:   msg,
		msgType:      eOutputMessageType.Prompt(),
		inputChannel: expectedInputChannel,
	}

	// block until input comes from the user
	return <-expectedInputChannel
}

// TODO minor: consider merging with Exit
func (lcm *lifecycleMgr) Error(msg string) {
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

func (lcm *lifecycleMgr) Exit(o OutputBuilder, exitCode ExitCode) {
	// Check if need to do memory profiling, and do memory profiling accordingly before azcopy exits.
	lcm.checkAndTriggerMemoryProfiling()

	// Check if there is ongoing CPU profiling, and stop CPU profiling.
	lcm.checkAndStopCPUProfiling()

	messageContent := ""
	if o != nil {
		messageContent = o(lcm.outputFormat)
	}

	lcm.msgQueue <- outputMessage{
		msgContent: messageContent,
		msgType:    eOutputMessageType.Exit(),
		exitCode:   exitCode,
	}

	// stall forever until the success message is printed and program exits
	lcm.SurrenderControl()
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
		switch msgToPrint := <-lcm.msgQueue; lcm.outputFormat {
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
	if msgToOutput.msgType == eOutputMessageType.Exit() {
		os.Exit(int(msgToOutput.exitCode))
	} else if msgToOutput.msgType == eOutputMessageType.Error() {
		os.Exit(int(EExitCode.Error()))
	}

	// ignore all other outputs
	return
}

func (lcm *lifecycleMgr) processJSONOutput(msgToOutput outputMessage) {
	msgType := msgToOutput.msgType

	// right now, we return nothing so that the default behavior is triggered for the part that intended to get response
	if msgType == eOutputMessageType.Prompt() {
		// TODO determine how prompts work with JSON output
		msgToOutput.inputChannel <- ""
		return
	}

	// simply output the json message
	// we assume the msgContent is already formatted correctly
	fmt.Println(GetJsonStringFromTemplate(newJsonOutputTemplate(msgType, msgToOutput.msgContent)))

	// exit if needed
	if msgType == eOutputMessageType.Exit() || msgType == eOutputMessageType.Error() {
		os.Exit(int(msgToOutput.exitCode))
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
	case eOutputMessageType.Error(), eOutputMessageType.Exit():
		// simply print and quit
		// if no message is intended, avoid adding new lines
		if msgToOutput.msgContent != "" {
			fmt.Println("\n" + msgToOutput.msgContent)
		}
		os.Exit(int(msgToOutput.exitCode))

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

		// read the response to the prompt and send it back through the channel
		msgToOutput.inputChannel <- lcm.readInCleanLineFromStdIn()
	}
}

// for the lifecycleMgr to babysit a job, it must be given a controller to get information about the job
type WorkController interface {
	Cancel(mgr LifecycleMgr)               // handle to cancel the work
	ReportProgressOrExit(mgr LifecycleMgr) // print the progress status, optionally exit the application if work is done
}

// isInteractive indicates whether the application was spawned by an actual user on the command
func (lcm *lifecycleMgr) InitiateProgressReporting(jc WorkController, isInteractive bool) {
	if !atomic.CompareAndSwapInt32(&lcm.waitEverCalled, 0, 1) {
		return
	}

	// this go routine never returns
	// it will terminate the whole process eventually when the work is complete
	go func() {
		// cancelChannel will be notified when os receives os.Interrupt and os.Kill signals
		signal.Notify(lcm.cancelChannel, os.Interrupt, os.Kill)

		// if the application was launched by another process, allow input from stdin to trigger a cancellation
		if !isInteractive {
			// dispatch a routine to read the stdin
			// if input is the word 'cancel' then stop the current job by sending a kill signal to cancel channel
			go func() {
				for {
					input := lcm.readInCleanLineFromStdIn()

					// if the word 'cancel' was passed in, then cancel the current job by sending a signal to the cancel channel
					if strings.EqualFold(input, "cancel") {
						// send a kill signal to the cancel channel.
						lcm.cancelChannel <- os.Kill

						// exit the loop as soon as cancel is received
						// there is no need to wait on the stdin anymore
						break
					}
				}
			}()
		}

		for {
			select {
			case <-lcm.cancelChannel:
				jc.Cancel(lcm)
			default:
				jc.ReportProgressOrExit(lcm)
			}

			// wait a bit before fetching job status again, as fetching has costs associated with it on the backend
			time.Sleep(2 * time.Second)
		}
	}()
}

// reads in a single line from stdin
// trims the new line, and also the extra spaces around the content
func (lcm *lifecycleMgr) readInCleanLineFromStdIn() string {
	consoleReader := bufio.NewReader(os.Stdin)

	// reads input until the first occurrence of \n in the input,
	input, err := consoleReader.ReadString('\n')
	// When the user cancel the job more than one time before providing the
	// input there will be an EOF Error.
	if err == io.EOF {
		return ""
	}

	// remove the delimiter "\n" and spaces before/after the content
	input = strings.TrimSpace(input)
	return strings.Trim(input, " ")
}

func (lcm *lifecycleMgr) GetEnvironmentVariable(env EnvironmentVariable) string {
	value := os.Getenv(env.Name)
	if value == "" {
		return env.DefaultValue
	}
	return value
}

// captures the common logic of exiting if there's an expected error
func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
