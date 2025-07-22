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
	"io"
	"log"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	datalakefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	sharefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
)

var AzcopyCurrentJobLogger ILoggerResetable

// TODO: (gapra) I think this should actually be a function on the logger?

// LogToJobLogWithPrefix logs a message with a prefix.
func LogToJobLogWithPrefix(msg string, level LogLevel) {
	if AzcopyCurrentJobLogger != nil {
		prefix := ""
		if level <= LogWarning {
			prefix = fmt.Sprintf("%s: ", level) // so readers can find serious ones, but information ones still look uncluttered without INFO:
		}
		AzcopyCurrentJobLogger.Log(level, prefix+msg)
	}
}

type ILogger interface {
	ShouldLog(level LogLevel) bool
	Log(level LogLevel, msg string)
	Panic(err error)
}

type ILoggerCloser interface {
	ILogger
	CloseLog()
}

type ILoggerResetable interface {
	OpenLog()
	MinimumLogLevel() LogLevel
	ILoggerCloser
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type LogLevelOverrideLogger struct {
	ILoggerResetable
	MinimumLevelToLog LogLevel
}

func (l LogLevelOverrideLogger) MinimumLogLevel() LogLevel {
	return l.MinimumLevelToLog
}

func (l LogLevelOverrideLogger) ShouldLog(level LogLevel) bool {
	if level == LogNone {
		return false
	}
	return level <= l.MinimumLevelToLog
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const maxLogSize = 500 * 1024 * 1024

type jobLogger struct {
	// maximum loglevel represents the maximum severity of log messages which can be logged to Job Log file.
	// any message with severity higher than this will be ignored.
	jobID             JobID
	minimumLevelToLog LogLevel       // The maximum customer-desired log level for this job
	file              io.WriteCloser // The job's log file
	logFileFolder     string         // The log file's parent folder, needed for opening the file at the right place
	logger            *log.Logger    // The Job's logger
	sanitizer         LogSanitizer
	logFileNameSuffix string // Used to allow more than 1 log per job, ex: front-end and back-end logs should be separate
}

func NewJobLogger(jobID JobID, minimumLevelToLog LogLevel, logFileFolder string, logFileNameSuffix string) ILoggerResetable {
	return &jobLogger{
		jobID:             jobID,
		minimumLevelToLog: minimumLevelToLog,
		logFileFolder:     logFileFolder,
		sanitizer:         NewAzCopyLogSanitizer(),
		logFileNameSuffix: logFileNameSuffix,
	}
}

func (jl *jobLogger) OpenLog() {
	if jl.minimumLevelToLog == LogNone {
		return
	}

	file, err := NewRotatingWriter(path.Join(jl.logFileFolder, jl.jobID.String()+jl.logFileNameSuffix+".log"), maxLogSize)
	PanicIfErr(err)

	jl.file = file

	flags := log.LstdFlags | log.LUTC
	utcMessage := fmt.Sprintf("Log times are in UTC. Local time is %s", time.Now().Format("2 Jan 2006 15:04:05"))

	jl.logger = log.New(jl.file, "", flags)
	// Log the Azcopy Version
	jl.logger.Println("AzcopyVersion ", AzcopyVersion)
	// Log the OS Environment and OS Architecture
	jl.logger.Println("OS-Environment ", runtime.GOOS)
	jl.logger.Println("OS-Architecture ", runtime.GOARCH)
	jl.logger.Println(utcMessage)
}

func (jl *jobLogger) MinimumLogLevel() LogLevel {
	return jl.minimumLevelToLog
}

func (jl *jobLogger) ShouldLog(level LogLevel) bool {
	if level == LogNone {
		return false
	}
	return level <= jl.minimumLevelToLog
}

func (jl *jobLogger) CloseLog() {
	if jl.minimumLevelToLog == LogNone {
		return
	}

	jl.logger.Println("Closing Log")
	_ = jl.file.Close() // If it was already closed, that's alright. We wanted to close it, anyway.
}

func (jl jobLogger) Log(loglevel LogLevel, msg string) {
	// If the logger for Job is not initialized i.e file is not open
	// or logger instance is not initialized, then initialize it

	// ensure all secrets are redacted
	msg = jl.sanitizer.SanitizeLogMessage(msg)

	// Go, and therefore the sdk, defaults to \n for line endings, so if the platform has a different line ending,
	// we should replace them to ensure readability on the given platform.
	if lineEnding != "\n" {
		msg = strings.Replace(msg, "\n", lineEnding, -1)
	}
	if jl.ShouldLog(loglevel) {
		jl.logger.Println(msg)
	}
}

func (jl jobLogger) Panic(err error) {
	jl.logger.Println(err) // We do NOT panic here as the app would terminate; we just log it
	panic(err)
	// We should never reach this line of code!
}

const TryEquals string = "Try=" // TODO: refactor so that this can be used by the retry policies too?  So that when you search the logs for Try= you are guaranteed to find both types of retry (i.e. request send retries, and body read retries)

func NewBlobReadLogFunc(logger ILogger, fullUrl string) func(int32, error, blob.HTTPRange, bool) {
	redactedUrl := URLStringExtension(fullUrl).RedactSecretQueryParamForLogging()

	return func(failureCount int32, err error, r blob.HTTPRange, willRetry bool) {
		retryMessage := "Will retry"
		if !willRetry {
			retryMessage = "Will NOT retry"
		}
		logger.Log(LogInfo, fmt.Sprintf(
			"Error reading body of reply. Next try (if any) will be %s%d. %s. Error: %s. Offset: %d  Count: %d URL: %s",
			TryEquals, // so that retry wording for body-read retries is similar to that for URL-hitting retries

			// We log the number of the NEXT try, not the failure just done, so that users searching the log for "Try=2"
			// will find ALL retries, both the request send retries (which are logged as try 2 when they are made) and
			// body read retries (for which only the failure is logged - so if we did the actual failure number, there would be
			// not Try=2 in the logs if the retries work).
			failureCount+1,

			retryMessage,
			err,
			r.Offset,
			r.Count,
			redactedUrl))
	}
}

func NewFileReadLogFunc(logger ILogger, fullUrl string) func(int32, error, sharefile.HTTPRange, bool) {
	redactedUrl := URLStringExtension(fullUrl).RedactSecretQueryParamForLogging()

	return func(failureCount int32, err error, r sharefile.HTTPRange, willRetry bool) {
		retryMessage := "Will retry"
		if !willRetry {
			retryMessage = "Will NOT retry"
		}
		logger.Log(LogInfo, fmt.Sprintf(
			"Error reading body of reply. Next try (if any) will be %s%d. %s. Error: %s. Offset: %d  Count: %d URL: %s",
			TryEquals, // so that retry wording for body-read retries is similar to that for URL-hitting retries

			// We log the number of the NEXT try, not the failure just done, so that users searching the log for "Try=2"
			// will find ALL retries, both the request send retries (which are logged as try 2 when they are made) and
			// body read retries (for which only the failure is logged - so if we did the actual failure number, there would be
			// not Try=2 in the logs if the retries work).
			failureCount+1,

			retryMessage,
			err,
			r.Offset,
			r.Count,
			redactedUrl))
	}
}

func NewDatalakeReadLogFunc(logger ILogger, fullUrl string) func(int32, error, datalakefile.HTTPRange, bool) {
	redactedUrl := URLStringExtension(fullUrl).RedactSecretQueryParamForLogging()

	return func(failureCount int32, err error, r datalakefile.HTTPRange, willRetry bool) {
		retryMessage := "Will retry"
		if !willRetry {
			retryMessage = "Will NOT retry"
		}
		logger.Log(LogInfo, fmt.Sprintf(
			"Error reading body of reply. Next try (if any) will be %s%d. %s. Error: %s. Offset: %d  Count: %d URL: %s",
			TryEquals, // so that retry wording for body-read retries is similar to that for URL-hitting retries

			// We log the number of the NEXT try, not the failure just done, so that users searching the log for "Try=2"
			// will find ALL retries, both the request send retries (which are logged as try 2 when they are made) and
			// body read retries (for which only the failure is logged - so if we did the actual failure number, there would be
			// not Try=2 in the logs if the retries work).
			failureCount+1,

			retryMessage,
			err,
			r.Offset,
			r.Count,
			redactedUrl))
	}
}

func IsForceLoggingDisabled() bool {
	return GetLifecycleMgr().IsForceLoggingDisabled()
}

type S3HTTPTraceLogger struct {
	logger   ILogger
	logLevel LogLevel
}

func NewS3HTTPTraceLogger(logger ILogger, level LogLevel) S3HTTPTraceLogger {
	return S3HTTPTraceLogger{
		logger:   logger,
		logLevel: level,
	}
}

func (e S3HTTPTraceLogger) Write(msg []byte) (n int, err error) {
	toPrint := string(msg)
	e.logger.Log(e.logLevel, toPrint)
	return len(toPrint), nil
}

type causer interface {
	Cause() error
}

// Cause walks all the preceding errors and return the originating error.
func Cause(err error) error {
	for err != nil {
		cause, ok := err.(causer)
		if !ok {
			break
		}
		err = cause.Cause()
	}
	return err
}
