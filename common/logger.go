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
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

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

type jobLogger struct {
	// maximum loglevel represents the maximum severity of log messages which can be logged to Job Log file.
	// any message with severity higher than this will be ignored.
	jobID             JobID
	minimumLevelToLog pipeline.LogLevel // The maximum customer-desired log level for this job
	file              *os.File          // The job's log file
	logFileFolder     string            // The log file's parent folder, needed for opening the file at the right place
	logger            *log.Logger       // The Job's logger
	sanitizer         pipeline.LogSanitizer
	logFileNameSuffix string // Used to allow more than 1 log per job, ex: front-end and back-end logs should be separate
}

func NewJobLogger(jobID JobID, minimumLevelToLog LogLevel, logFileFolder string, logFileNameSuffix string) ILoggerResetable {
	return &jobLogger{
		jobID:             jobID,
		minimumLevelToLog: minimumLevelToLog.ToPipelineLogLevel(),
		logFileFolder:     logFileFolder,
		sanitizer:         NewAzCopyLogSanitizer(),
		logFileNameSuffix: logFileNameSuffix,
	}
}

func (jl *jobLogger) OpenLog() {
	if jl.minimumLevelToLog == pipeline.LogNone {
		return
	}

	file, err := os.OpenFile(path.Join(jl.logFileFolder, jl.jobID.String()+jl.logFileNameSuffix+".log"),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, DEFAULT_FILE_PERM)
	PanicIfErr(err)

	jl.file = file

	flags := log.LstdFlags | log.LUTC
	utcMessage := fmt.Sprintf("Log times are in UTC. Local time is " + time.Now().Format("2 Jan 2006 15:04:05"))

	jl.logger = log.New(jl.file, "", flags)
	// Log the Azcopy Version
	jl.logger.Println("AzcopyVersion ", AzcopyVersion)
	// Log the OS Environment and OS Architecture
	jl.logger.Println("OS-Environment ", runtime.GOOS)
	jl.logger.Println("OS-Architecture ", runtime.GOARCH)
	jl.logger.Println(utcMessage)
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
	if jl.minimumLevelToLog == pipeline.LogNone {
		return
	}

	jl.logger.Println("Closing Log")
	err := jl.file.Close()
	PanicIfErr(err)
}

func (jl jobLogger) Log(loglevel pipeline.LogLevel, msg string) {
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

func NewReadLogFunc(logger ILogger, fullUrl *url.URL) func(int, error, int64, int64, bool) {
	redactedUrl := URLStringExtension(fullUrl.String()).RedactSecretQueryParamForLogging()

	return func(failureCount int, err error, offset int64, count int64, willRetry bool) {
		retryMessage := "Will retry"
		if !willRetry {
			retryMessage = "Will NOT retry"
		}
		logger.Log(pipeline.LogInfo, fmt.Sprintf(
			"Error reading body of reply. Next try (if any) will be %s%d. %s. Error: %s. Offset: %d  Count: %d URL: %s",
			TryEquals, // so that retry wording for body-read retries is similar to that for URL-hitting retries

			// We log the number of the NEXT try, not the failure just done, so that users searching the log for "Try=2"
			// will find ALL retries, both the request send retries (which are logged as try 2 when they are made) and
			// body read retries (for which only the failure is logged - so if we did the actual failure number, there would be
			// not Try=2 in the logs if the retries work).
			failureCount+1,

			retryMessage,
			err,
			offset,
			count,
			redactedUrl))
	}
}

func IsForceLoggingDisabled() bool {
	return GetLifecycleMgr().IsForceLoggingDisabled()
}

type S3HTTPTraceLogger struct {
	logger   ILogger
	logLevel pipeline.LogLevel
}

func NewS3HTTPTraceLogger(logger ILogger, level pipeline.LogLevel) S3HTTPTraceLogger {
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
