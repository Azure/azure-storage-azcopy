package common

import (
	"fmt"
	"log"
	"os"
)

type LogLevel uint8

const (
	// LogNone tells a logger not to log any entries passed to it.
	LogNone LogLevel = iota

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

// Logger is struct holding Information of log file for specific Job
// Each Job has its own logger instance. For all the parts of same Job, logs are logged in one file
type Logger struct {
	Severity    LogLevel
	LogFileName string
	LogFile     *os.File
}

// Initializes the logger instance for given JobId with given Log Severity
func (logger *Logger) Initialize(severity LogLevel, fileName string) {
	logger.Severity = severity
	logger.LogFileName = fileName
	// Creates the log file if it does not exists already else opens the file in append mode.
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	logger.LogFile = file
}

// TODO take severity as argument
// TODO rename to Logf
// Debug writes the debug level logs to the file.
func (logger *Logger) Debug(format string, a ...interface{}) {
	// If the severity of current logger instance is less than DEBUG, then logs will not logged
	if logger.Severity < LogInfo {
		return
	}
	log.SetOutput(logger.LogFile)
	debugMsg := fmt.Sprintf("DEBUG: "+format, a...)
	log.Println(debugMsg)
}

// Info writes the info level logs to the file.
func (logger *Logger) Info(format string, a ...interface{}) {
	// If the severity of current logger instance is less than INFO, then logs will not logged
	if logger.Severity < LogInfo {
		return
	}
	log.SetOutput(logger.LogFile)
	infoMsg := fmt.Sprintf("INFO: "+format, a...)
	log.Println(infoMsg)
}

// Error writes the Error level logs to the file.
func (logger *Logger) Error(format string, a ...interface{}) {
	// Error logs are always logged to the file irrespective to the severity of current logger instance.
	log.SetOutput(logger.LogFile)
	errorMsg := fmt.Sprintf("ERROR: "+format, a...)
	log.Println(errorMsg)
}
