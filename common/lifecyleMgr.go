package common

// TODO : Refactor these names - just left them as is to reduce code churn at this moment in this PR.
var lcm *JobOutputHandler

func SetOutputHandler(handler *JobOutputHandler) {
	lcm = handler
}

func GetLifecycleMgr() *JobOutputHandler {
	return lcm
}

// captures the common logic of exiting if there's an expected error
func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
