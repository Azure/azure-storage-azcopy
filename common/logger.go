// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package common

import (
	"log"
	"os"
	"runtime"

	"path"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

type ILogger interface {
	ShouldLog(level pipeline.LogLevel) bool
	Log(level pipeline.LogLevel, msg string)
	Panic(err error)
}

type ILoggerCloser interface {
	ILogger
	CloseLog()
}

type ILoggerResetable interface {
	OpenLog()
	MinimumLogLevel() pipeline.LogLevel
	ILoggerCloser
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func NewAppLogger(minimumLevelToLog pipeline.LogLevel, logFileFolder string) ILoggerCloser {
	// TODO: Put start date time in file name
	// TODO: log life time management.
	//appLogFile, err := os.OpenFile(path.Join(logFileFolder, "azcopy.log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666) // TODO: Make constant for 0666
	//PanicIfErr(err)
	return &appLogger{
		minimumLevelToLog: minimumLevelToLog,
		//file:              appLogFile,
		//logger:            log.New(appLogFile, "", log.LstdFlags|log.LUTC),
	}
}

type appLogger struct {
	// maximum loglevel represents the maximum severity of log messages which can be logged to Job Log file.
	// any message with severity higher than this will be ignored.
	minimumLevelToLog pipeline.LogLevel // The maximum customer-desired log level for this job
	file              *os.File          // The job's log file
	logger            *log.Logger       // The Job's logger
}

func (al *appLogger) ShouldLog(level pipeline.LogLevel) bool {
	if level == pipeline.LogNone {
		return false
	}
	return level <= al.minimumLevelToLog
}

func (al *appLogger) CloseLog() {
	// TODO consider delete completely to get rid of app logger
	//al.logger.Println("Closing Log")
	//err := al.file.Close()
	//PanicIfErr(err)
}

func (al *appLogger) Log(loglevel pipeline.LogLevel, msg string) {
	// TODO consider delete completely to get rid of app logger
	//if al.ShouldLog(loglevel) {
	//	al.logger.Println(msg)
	//}
}

func (al *appLogger) Panic(err error) {
	// TODO consider delete completely to get rid of app logger
	//al.logger.Panic(err)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type jobLogger struct {
	// maximum loglevel represents the maximum severity of log messages which can be logged to Job Log file.
	// any message with severity higher than this will be ignored.
	jobID             JobID
	minimumLevelToLog pipeline.LogLevel // The maximum customer-desired log level for this job
	file              *os.File          // The job's log file
	logFileFolder     string            // The log file's parent folder, needed for opening the file at the right place
	logger            *log.Logger       // The Job's logger
	appLogger         ILogger
}

func NewJobLogger(jobID JobID, minimumLevelToLog LogLevel, appLogger ILogger, logFileFolder string) ILoggerResetable {
	if appLogger == nil {
		panic("You must pass a appLogger when creating a JobLogger")
	}

	return &jobLogger{
		jobID:             jobID,
		appLogger:         appLogger, // Panics are recorded in the job log AND in the app log
		minimumLevelToLog: minimumLevelToLog.ToPipelineLogLevel(),
		logFileFolder:     logFileFolder,
	}
}

func (jl *jobLogger) OpenLog() {
	file, err := os.OpenFile(path.Join(jl.logFileFolder, jl.jobID.String()+".log"),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666) // TODO: Make constant for 0666
	PanicIfErr(err)

	jl.file = file
	jl.logger = log.New(jl.file, "", log.LstdFlags|log.LUTC)
	// Log the Azcopy Version
	jl.logger.Println("AzcopVersion ", AzcopyVersion)
	// Log the OS Environment and OS Architecture
	jl.logger.Println("OS-Environment ", runtime.GOOS)
	jl.logger.Println("OS-Architecture ", runtime.GOARCH)
}

func (jl *jobLogger) MinimumLogLevel() pipeline.LogLevel {
	return jl.minimumLevelToLog
}

func (jl *jobLogger) ShouldLog(level pipeline.LogLevel) bool {
	if level == pipeline.LogNone {
		return false
	}
	return level <= jl.minimumLevelToLog
}

func (jl *jobLogger) CloseLog() {
	jl.logger.Println("Closing Log")
	err := jl.file.Close()
	PanicIfErr(err)
}

func (jl jobLogger) Log(loglevel pipeline.LogLevel, msg string) {
	// If the logger for Job is not initialized i.e file is not open
	// or logger instance is not initialized, then initialize it
	if jl.ShouldLog(loglevel) {
		jl.logger.Println(msg)
	}
}

func (jl jobLogger) Panic(err error) {
	jl.logger.Println(err)  // We do NOT panic here as the app would terminate; we just log it
	jl.appLogger.Panic(err) // We panic here that it logs and the app terminates
	// We should never reach this line of code!
}
