package common

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
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

	return
}()

// create a public interface so that consumers outside of this package can refer to the lifecycle manager
// but they would not be able to instantiate one
type LifecycleMgr interface {
	Progress(string)
	Info(string)
	ExitWithSuccess(string, ExitCode)
	ExitWithError(string, ExitCode)
	SurrenderControl()
	ReadStandardInputToCancelJob()
	WaitUntilJobCompletion(JobController)
}

func GetLifecycleMgr() LifecycleMgr {
	return lcm
}

var eMessageType = outputMessageType(0)

// outputMessageType defines the nature of the output, ex: progress report, job summary, or error
type outputMessageType uint8

func (outputMessageType) Progress() outputMessageType { return outputMessageType(0) } // should be printed on the same line over and over again, not allowed to float up
func (outputMessageType) Info() outputMessageType     { return outputMessageType(1) } // simple print, allowed to float up
func (outputMessageType) Success() outputMessageType  { return outputMessageType(2) } // exit after printing
func (outputMessageType) Error() outputMessageType    { return outputMessageType(3) } // always fatal, exit after printing

// defines the output and how it should be handled
type outputMessage struct {
	msgContent string
	msgType    outputMessageType
	exitCode   ExitCode // only for when the application is meant to exit after printing (i.e. Error or Final)
}

// single point of control for all outputs
type lifecycleMgr struct {
	msgQueue      chan outputMessage
	progressCache string // useful for keeping job progress on the last line
	cancelChannel chan os.Signal
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

func (lcm *lifecycleMgr) ExitWithSuccess(msg string, exitCode ExitCode) {
	lcm.msgQueue <- outputMessage{
		msgContent: msg,
		msgType:    eMessageType.Success(),
		exitCode:   exitCode,
	}

	// stall forever until the success message is printed and program exits
	lcm.SurrenderControl()
}

func (lcm *lifecycleMgr) ExitWithError(msg string, exitCode ExitCode) {
	lcm.msgQueue <- outputMessage{
		msgContent: msg,
		msgType:    eMessageType.Error(),
		exitCode:   exitCode,
	}

	// stall forever until the error message is printed and program exits
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
		case eMessageType.Error():
			// simply print and quit
			fmt.Println("\n" + "FATAL ERROR: " + msgToPrint.msgContent)
			os.Exit(int(msgToPrint.exitCode))

		case eMessageType.Success():
			// simply print and quit
			fmt.Println(msgToPrint.msgContent)
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
		}
	}
}

// ReadStandardInputToCancelJob is a function that reads the standard Input
// If Input given is "cancel", it cancels the current job.
func (lcm *lifecycleMgr) ReadStandardInputToCancelJob() {
	for {
		consoleReader := bufio.NewReader(os.Stdin)
		// ReadString reads input until the first occurrence of \n in the input,
		input, err := consoleReader.ReadString('\n')
		if err != nil {
			return
		}

		//remove the delimiter "\n"
		input = strings.Trim(input, "\n")
		// remove trailing white spaces
		input = strings.Trim(input, " ")
		// converting the input characters to lower case characters
		// this is done to avoid case sensitiveness.
		input = strings.ToLower(input)

		switch input {
		case "cancel":
			// send a kill signal to the cancel channel.
			lcm.cancelChannel <- os.Kill
		default:
			panic(fmt.Errorf("command %s not supported by azcopy", input))
		}
	}
}

// for the lifecycleMgr to babysit a job, it must be given a controller to get information about the job
type JobController interface {
	PrintJobStartedMsg()         // print an initial message to indicate that the work has started
	CancelJob()                  // handle to cancel the work
	InitializeProgressCounters() // initialize states needed to track progress (such as start time of the work)
	PrintJobProgressStatus()     // print the progress status, optionally exit the application if work is done
}

func (lcm *lifecycleMgr) WaitUntilJobCompletion(jc JobController) {
	// CancelChannel will be notified when os receives os.Interrupt and os.Kill signals
	// waiting for signals from either CancelChannel or timeOut Channel.
	// if no signal received, will fetch/display a job status update then sleep for a bit
	signal.Notify(lcm.cancelChannel, os.Interrupt, os.Kill)

	// print message to indicate work has started
	jc.PrintJobStartedMsg()

	// set up the job controller so that it's ready to report progress of the job
	jc.InitializeProgressCounters()

	for {
		select {
		case <-lcm.cancelChannel:
			jc.CancelJob()
		default:
			jc.PrintJobProgressStatus()
		}

		// wait a bit before fetching job status again, as fetching has costs associated with it on the backend
		time.Sleep(2 * time.Second)
	}

}
