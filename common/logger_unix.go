//go:build linux || darwin
// +build linux darwin

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
	"log/syslog"
	"runtime"
)

// ////////////////////////////////////////
type sysLogger struct {
	// minimum loglevel represents the minimum severity of log messages which can be logged to Job Log file.
	// any message with severity higher than this will be ignored.
	jobID             JobID
	minimumLevelToLog LogLevel       // The maximum customer-desired log level for this job
	writer            *syslog.Writer // The Job's logger
	logSuffix         string
	sanitizer         LogSanitizer
}

func NewSysLogger(jobID JobID, minimumLevelToLog LogLevel, logSuffix string) ILoggerResetable {
	return &sysLogger{
		jobID:             jobID,
		minimumLevelToLog: minimumLevelToLog,
		logSuffix:         logSuffix,
		sanitizer:         NewAzCopyLogSanitizer(),
	}
}

func (sl *sysLogger) OpenLog() {
	if sl.minimumLevelToLog == LogNone {
		return
	}
	writer, err := syslog.New(syslog.LOG_NOTICE, fmt.Sprintf("%s %s", sl.logSuffix, sl.jobID.String()))
	PanicIfErr(err)

	sl.writer = writer
	// Log the Azcopy Version
	_ = sl.writer.Notice("AzcopyVersion " + AzcopyVersion)
	// Log the OS Environment and OS Architecture
	_ = sl.writer.Notice("OS-Environment " + runtime.GOOS)
	_ = sl.writer.Notice("OS-Architecture " + runtime.GOARCH)
}

func (sl *sysLogger) MinimumLogLevel() LogLevel {
	return sl.minimumLevelToLog
}

func (jl *sysLogger) ShouldLog(level LogLevel) bool {
	if level == LogNone {
		return false
	}
	return level <= jl.minimumLevelToLog
}

// XDM: This update is not necessarily safe from multiple goroutines simultaneously calling it.
// Typically we will call ChangeLogLevel() once at the beginning so it should be ok.
func (sl *sysLogger) ChangeLogLevel(level LogLevel) {
	if level == LogNone {
		return
	}
	sl.minimumLevelToLog = level
	return
}

func (sl *sysLogger) CloseLog() {
	if sl.minimumLevelToLog == LogNone {
		return
	}

	_ = sl.writer.Notice("Closing Log")
	sl.writer.Close()
}

func (sl *sysLogger) Panic(err error) {
	_ = sl.writer.Crit(err.Error()) // We do NOT panic here as the app would terminate;
	//we just log it. We should never reach this line of code!
}

func (sl *sysLogger) Log(loglevel LogLevel, msg string) {
	if !sl.ShouldLog(loglevel) {
		return
	}
	w := sl.writer
	// ensure all secrets are redacted
	msg = sl.sanitizer.SanitizeLogMessage(msg)

	switch loglevel {
	case LogNone:
		//nothing to do
	case LogFatal:
		_ = w.Emerg(msg)
	case LogPanic:
		_ = w.Crit(msg)
	case LogError:
		_ = w.Err(msg)
	case LogWarning:
		_ = w.Warning(msg)
	case LogInfo:
		_ = w.Info(msg)
	case LogDebug:
		_ = w.Debug(msg)
	}
}
