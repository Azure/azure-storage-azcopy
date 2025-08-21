package common

var lcm JobLifecycleHandler

// SetJobLifecycleHandler allows AzCopy CLI and AzCopy as a library to set a custom JobLifecycleHandler
// Be careful when using this function, as it will replace the existing lifecycle manager.
func SetJobLifecycleHandler(handler JobLifecycleHandler) {
	lcm = handler
}

func GetLifecycleMgr() JobLifecycleHandler {
	return lcm
}

// create a public interface so that consumers outside of this package can refer to the lifecycle manager
// but they would not be able to instantiate one
type LifecycleMgr interface {
	JobLifecycleHandler
	// TODO (gapra) : For copy, sync, resume - we use the OnComplete method to support AzCopy as a library.
	// I honestly don't think we need this in an ideal simple implementation of AzCopy, but it is used in the code and will take some rework and testing to fully remove.
	Exit(OutputBuilder, ExitCode)             // indicates successful execution exit after printing, allow user to specify exit code
	Dryrun(OutputBuilder)                     // print files for dry run mode
	Output(OutputBuilder, OutputMessageType)  // print output for list
	SurrenderControl()                        // give up control, this should never return
	InitiateProgressReporting(WorkController) // start writing progress with another routine
	AllowReinitiateProgressReporting()        // allow re-initiation of progress reporting for followup job
	SetOutputFormat(OutputFormat)             // change the output format of the entire application
	EnableInputWatcher()                      // depending on the command, we may allow user to give input through Stdin
	EnableCancelFromStdIn()                   // allow user to send in `cancel` to stop the job
	E2EAwaitContinue()                        // used by E2E tests
	E2EEnableAwaitAllowOpenFiles(enable bool) // used by E2E tests
	MsgHandlerChannel() <-chan *LCMMsg
	ReportAllJobPartsDone()
	SetOutputVerbosity(mode OutputVerbosity)
}

// for the lifecycleMgr to babysit a job, it must be given a controller to get information about the job
type WorkController interface {
	Cancel(mgr LifecycleMgr)                                        // handle to cancel the work
	ReportProgressOrExit(mgr LifecycleMgr) (totalKnownCount uint32) // print the progress status, optionally exit the application if work is done
}

// captures the common logic of exiting if there's an expected error
func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
