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
	Progress(string)                                // print on the same line over and over again, not allowed to float up
	Info(string)                                    // simple print, allowed to float up
	Prompt(string) string                           // ask the user a question(after erasing the progress), then return the response
	Exit(string, ExitCode)                          // exit after printing
	SurrenderControl()                              // give up control, this should never return
	InitiateProgressReporting(WorkController, bool) // start writing progress with another routine
}

func GetLifecycleMgr() LifecycleMgr {
	return lcm
}

var eMessageType = outputMessageType(0)

// outputMessageType defines the nature of the output, ex: progress report, job summary, or error
type outputMessageType uint8

func (outputMessageType) Progress() outputMessageType { return outputMessageType(0) } // should be printed on the same line over and over again, not allowed to float up
func (outputMessageType) Info() outputMessageType     { return outputMessageType(1) } // simple print, allowed to float up
func (outputMessageType) Exit() outputMessageType     { return outputMessageType(2) } // exit after printing
func (outputMessageType) Prompt() outputMessageType   { return outputMessageType(3) } // ask the user a question after erasing the progress

// defines the output and how it should be handled
type outputMessage struct {
	msgContent   string
	msgType      outputMessageType
	exitCode     ExitCode      // only for when the application is meant to exit after printing (i.e. Error or Final)
	inputChannel chan<- string // support getting a response from the user
}

// single point of control for all outputs
type lifecycleMgr struct {
	msgQueue       chan outputMessage
	progressCache  string // useful for keeping job progress on the last line
	cancelChannel  chan os.Signal
	waitEverCalled int32
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
			lcm.Exit(fmt.Sprintf("Fail to create file for CPU profiling, %v", err), EExitCode.Error())
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			lcm.Exit(fmt.Sprintf("Fail to start CPU profiling, %v", err), EExitCode.Error())
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
			lcm.Exit(fmt.Sprintf("Fail to create file for memory profiling, %v", err), EExitCode.Error())
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			lcm.Exit(fmt.Sprintf("Fail to start memory profiling, %v", err), EExitCode.Error())
		}
		if err := f.Close(); err != nil {
			lcm.Info(fmt.Sprintf("Fail to close memory profiling file, %v", err))
		}
	}
}

func (lcm *lifecycleMgr) Progress(msg string) {
	lcm.msgQueue <- outputMessage{
		msgContent: msg,
		msgType:    eMessageType.Progress(),
	}
}

func (lcm *lifecycleMgr) Info(msg string) {
	lcm.msgQueue <- outputMessage{
		msgContent: msg,
		msgType:    eMessageType.Info(),
	}
}

func (lcm *lifecycleMgr) Prompt(msg string) string {
	expectedInputChannel := make(chan string, 1)
	lcm.msgQueue <- outputMessage{
		msgContent:   msg,
		msgType:      eMessageType.Prompt(),
		inputChannel: expectedInputChannel,
	}

	// block until input comes from the user
	return <-expectedInputChannel
}

func (lcm *lifecycleMgr) Exit(msg string, exitCode ExitCode) {
	// Check if need to do memory profiling, and do memory profiling accordingly before azcopy exits.
	lcm.checkAndTriggerMemoryProfiling()

	// Check if there is ongoing CPU profiling, and stop CPU profiling.
	lcm.checkAndStopCPUProfiling()

	lcm.msgQueue <- outputMessage{
		msgContent: msg,
		msgType:    eMessageType.Exit(),
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
	// when a new line needs to overwrite the current line completely
	// we need to make sure that if the new line is shorter, we properly erase everything from the current line
	var matchLengthWithSpaces = func(curLineLength, newLineLength int) {
		if dirtyLeftover := curLineLength - newLineLength; dirtyLeftover > 0 {
			for i := 0; i < dirtyLeftover; i++ {
				fmt.Print(" ")
			}
		}
	}

	// NOTE: fmt.printf is being avoided on purpose (for memory optimization)
	for {
		switch msgToPrint := <-lcm.msgQueue; msgToPrint.msgType {
		case eMessageType.Exit():
			// simply print and quit
			// if no message is intended, avoid adding new lines
			if msgToPrint.msgContent != "" {
				fmt.Println("\n" + msgToPrint.msgContent)
			}
			os.Exit(int(msgToPrint.exitCode))

		case eMessageType.Progress():
			fmt.Print("\r")                  // return carriage back to start
			fmt.Print(msgToPrint.msgContent) // print new progress

			// it is possible that the new progress status is somehow shorter than the previous one
			// in this case we must erase the left over characters from the previous progress
			matchLengthWithSpaces(len(lcm.progressCache), len(msgToPrint.msgContent))

			lcm.progressCache = msgToPrint.msgContent

		case eMessageType.Info():
			if lcm.progressCache != "" { // a progress status is already on the last line
				// print the info from the beginning on current line
				fmt.Print("\r")
				fmt.Print(msgToPrint.msgContent)

				// it is possible that the info is shorter than the progress status
				// in this case we must erase the left over characters from the progress status
				matchLengthWithSpaces(len(lcm.progressCache), len(msgToPrint.msgContent))

				// print the previous progress status again, so that it's on the last line
				fmt.Print("\n")
				fmt.Print(lcm.progressCache)
			} else {
				fmt.Println(msgToPrint.msgContent)
			}

		case eMessageType.Prompt():
			if lcm.progressCache != "" { // a progress status is already on the last line
				// print the prompt from the beginning on current line
				fmt.Print("\r")
				fmt.Print(msgToPrint.msgContent)

				// it is possible that the prompt is shorter than the progress status
				// in this case we must erase the left over characters from the progress status
				matchLengthWithSpaces(len(lcm.progressCache), len(msgToPrint.msgContent))

			} else {
				fmt.Print(msgToPrint.msgContent)
			}

			// read the response to the prompt and send it back through the channel
			msgToPrint.inputChannel <- lcm.readInCleanLineFromStdIn()
		}
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

// captures the common logic of exiting if there's an expected error
func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
