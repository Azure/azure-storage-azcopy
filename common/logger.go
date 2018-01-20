package common

import (
	"fmt"
	"log"
	"os"
)
type LogSeverity uint8
const (
	LOG_ERROR_LEVEL LogSeverity = 0
	LOG_INFO_LEVEL  LogSeverity = 1
	LOG_DEBUG_LEVEL LogSeverity = 2
)

type Logger struct{
	Severity LogSeverity
	LogFileName string
	LogFile *os.File
}

func (logger *Logger) Initialize(severity LogSeverity, jobID JobID){
	logger.Severity = severity
	fileName := fmt.Sprintf("%s.log", jobID)
	logger.LogFileName = fileName
	file, err := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil{
		panic(err)
	}
	logger.LogFile = file
}

func (logger *Logger) Debug(format string, a ...interface{}){
	if logger.Severity != LOG_DEBUG_LEVEL{
		return
	}
	log.SetOutput(logger.LogFile)
	debugMsg := fmt.Sprintf("DEBUG: " + format, a...)
	log.Println(debugMsg)
}

func (logger *Logger) Info(format string, a ...interface{}){
	log.SetOutput(logger.LogFile)
	infoMsg := fmt.Sprintf("INFO: " + format, a...)
	log.Println(infoMsg)
}

//func (logger *Logger) Warn(format string, a ...interface{}){
//	if logger.Severity != LOG_WARN_LEVEL {
//		return
//	}
//	log.SetOutput(logger.LogFile)
//	warnMsg := fmt.Sprintf(format, a...)
//	log.Println(warnMsg)
//}

func (logger *Logger) Error(format string, a ...interface{}){
	log.SetOutput(logger.LogFile)
	errorMsg := fmt.Sprintf("ERROR: " + format, a...)
	log.Println(errorMsg)
}
