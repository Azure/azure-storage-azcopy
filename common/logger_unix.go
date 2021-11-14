// +build linux darwin
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
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"
	"log/syslog"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

//////////////////////////////////////////
type sysLogger struct {
	// maximum loglevel represents the maximum severity of log messages which can be logged to Job Log file.
	// any message with severity higher than this will be ignored.
	jobID             JobID
	minimumLevelToLog pipeline.LogLevel // The maximum customer-desired log level for this job
	logger            *log.Logger       // The Job's logger
	logSuffix         string
	sanitizer         pipeline.LogSanitizer
}


func NewSysLogger(jobID JobID, minimumLevelToLog LogLevel, logSuffix string) ILoggerResetable {
	return &sysLogger{
		jobID:             jobID,
		minimumLevelToLog: minimumLevelToLog.ToPipelineLogLevel(),
		logSuffix:         logSuffix, 
		sanitizer:         NewAzCopyLogSanitizer(),
	}
}

func (sl *sysLogger) OpenLog() {
    logger, err := syslog.New(syslog.LOG_NOTICE, fmt.Sprintf("%s %s", sl.logSuffix, sl.jobID.String()))
    if err != nil {
        log.Fatalln(err)
    }

    flags := log.LstdFlags | log.LUTC
    sl.logger = log.New(logger, "", flags)
    utcMessage := fmt.Sprintf("Log times are in UTC. Local time is " + time.Now().Format("2 Jan 2006 15:04:05"))

    // Log the Azcopy Version
    sl.logger.Println("AzcopyVersion ", AzcopyVersion)
    // Log the OS Environment and OS Architecture
    sl.logger.Println("OS-Environment ", runtime.GOOS)
    sl.logger.Println("OS-Architecture ", runtime.GOARCH)
    sl.logger.Println(utcMessage)
}

func (sl *sysLogger) MinimumLogLevel() pipeline.LogLevel {
	return sl.minimumLevelToLog
}

func (jl *sysLogger) ShouldLog(level pipeline.LogLevel) bool {
	if level == pipeline.LogNone {
		return false
	}
	return level <= jl.minimumLevelToLog
}

func (sl *sysLogger) CloseLog() {
	if sl.minimumLevelToLog == pipeline.LogNone {
		return
	}

	sl.logger.Println("Closing Log")
}


func (sl *sysLogger) Panic(err error) {
	sl.logger.Println(err)  // We do NOT panic here as the app would terminate; we just log it
	// We should never reach this line of code!
}

func (sl *sysLogger) Log(loglevel pipeline.LogLevel, msg string) {
	// If the logger for Job is not initialized i.e file is not open
	// or logger instance is not initialized, then initialize it

	// ensure all secrets are redacted
	msg = sl.sanitizer.SanitizeLogMessage(msg)

	// Go, and therefore the sdk, defaults to \n for line endings, so if the platform has a different line ending,
	// we should replace them to ensure readability on the given platform.
	if lineEnding != "\n" {
		msg = strings.Replace(msg, "\n", lineEnding, -1)
	}
	if sl.ShouldLog(loglevel) {
		sl.logger.Println(msg)
	}
}