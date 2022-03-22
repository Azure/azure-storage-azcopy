
// Copyright Microsoft <wastore@microsoft.com>
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
	"runtime"
	"log/syslog"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

//////////////////////////////////////////
type sysLogger struct {
	// maximum loglevel represents the maximum severity of log messages which can be logged to Job Log file.
	// any message with severity higher than this will be ignored.
	jobID             JobID
	minimumLevelToLog pipeline.LogLevel // The maximum customer-desired log level for this job
	writer            *syslog.Writer      // The Job's logger
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
    writer, err := syslog.New(syslog.LOG_NOTICE, fmt.Sprintf("%s %s", sl.logSuffix, sl.jobID.String()))
    PanicIfErr(err)

    sl.writer = writer
    // Log the Azcopy Version
    sl.writer.Notice("AzcopyVersion " + AzcopyVersion)
    // Log the OS Environment and OS Architecture
    sl.writer.Notice("OS-Environment " + runtime.GOOS)
    sl.writer.Notice("OS-Architecture " + runtime.GOARCH)
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

	sl.writer.Notice("Closing Log")
	sl.writer.Close()
}


func (sl *sysLogger) Panic(err error) {
	sl.writer.Crit(err.Error())  // We do NOT panic here as the app would terminate;
	//we just log it. We should never reach this line of code!
}

func (sl *sysLogger) Log(loglevel pipeline.LogLevel, msg string) {
	if !sl.ShouldLog(loglevel) {
		return
	}
	w := sl.writer
	// ensure all secrets are redacted
	msg = sl.sanitizer.SanitizeLogMessage(msg)

	switch loglevel {
	case pipeline.LogNone:
		//nothing to do
	case pipeline.LogFatal:
		w.Emerg(msg)
	case pipeline.LogPanic:
		w.Crit(msg)
	case pipeline.LogError:
		w.Err(msg)
	case pipeline.LogWarning:
		w.Warning(msg)
	case pipeline.LogInfo:
		w.Info(msg)
	case pipeline.LogDebug:
		w.Debug(msg)
	}
}