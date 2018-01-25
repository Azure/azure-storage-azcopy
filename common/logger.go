package common

import (
	"fmt"
	"log"
	"os"
)

type LogSeverity uint8

// TODO reuse go-sdk's definitions
const (
	LOG_ERROR_LEVEL LogSeverity = 0
	LOG_INFO_LEVEL  LogSeverity = 1
	LOG_DEBUG_LEVEL LogSeverity = 2
)

// Logger is struct holding Information of log file for specific Job
// Each Job has its own logger instance. For all the parts of same Job, logs are logged in one file
type Logger struct {
	Severity    LogSeverity
	LogFileName string
	LogFile     *os.File
}

// Initializes the logger instance for given JobId with given Log Severity
func (logger *Logger) Initialize(severity LogSeverity, jobID JobID) {
	logger.Severity = severity
	fileName := fmt.Sprintf("%s.log", jobID)
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
	if logger.Severity <= LOG_DEBUG_LEVEL {
		return
	}
	log.SetOutput(logger.LogFile)
	debugMsg := fmt.Sprintf("DEBUG: "+format, a...)
	log.Println(debugMsg)
}

// Info writes the info level logs to the file.
func (logger *Logger) Info(format string, a ...interface{}) {
	// If the severity of current logger instance is less than INFO, then logs will not logged
	if logger.Severity <= LOG_INFO_LEVEL {
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
