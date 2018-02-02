package common

import (
	"fmt"
	"log"
	"os"
	"github.com/Azure/azure-pipeline-go/pipeline"
)

const (
	// LogNone tells a logger not to log any entries passed to it.
	LogNone pipeline.LogLevel = iota

	// LogFatal tells a logger to log all LogFatal entries passed to it.
	LogFatal

	// LogPanic tells a logger to log all LogPanic and LogFatal entries passed to it.
	LogPanic

	// LogError tells a logger to log all LogError, LogPanic and LogFatal entries passed to it.
	LogError

	// LogWarning tells a logger to log all LogWarning, LogError, LogPanic and LogFatal entries passed to it.
	LogWarning

	// LogInfo tells a logger to log all LogInfo, LogWarning, LogError, LogPanic and LogFatal entries passed to it.
	LogInfo
)

// logLevelToString converts the Loglevel severity to appropriate loglevel string
func logLevelToString(level pipeline.LogLevel) (string){
	switch level {
	case LogNone:
		return ""
	case LogFatal:
		return "ERROR"
	case LogPanic:
		return "ERROR"
	case LogError:
		return "ERROR"
	case LogWarning:
		return "WARNING"
	case LogInfo:
		return "INFO"
	default:
		return ""
	}
}

// Logger is struct holding Information of log file for specific Job
// Each Job has its own logger instance. For all the parts of same Job, logs are logged in one file
type Logger struct {
	Severity    pipeline.LogLevel
	LogFileName string
	LogFile     *os.File
}

// Initializes the logger instance for given JobId with given Log Severity
func (logger *Logger) Initialize(severity pipeline.LogLevel, fileName string) {
	logger.Severity = severity
	logger.LogFileName = fileName
	// Creates the log file if it does not exists already else opens the file in append mode.
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	logger.LogFile = file
}

// Logf api checks the log severity of current logger instance and writes the given
// logs to the filename of the current logger instance.
// If log severity of current logger instance is less than given severity, no logs
// will be written to the log file
func (logger *Logger) Logf(severity pipeline.LogLevel, format string, a ...interface{}){
	if severity > logger.Severity{
		return
	}
	log.SetOutput(logger.LogFile)
	logMsg := fmt.Sprintf(logLevelToString(severity) + ":"+format, a...)
	log.Println(logMsg)
}
