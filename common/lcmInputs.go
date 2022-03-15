package common

import "time"

type InputMsgType uint16

var EInputMsgType InputMsgType

func(InputMsgType) Invalid()                                InputMsgType {return InputMsgType(0)}
func(InputMsgType) CancelJob()                              InputMsgType {return InputMsgType(1)}
func(InputMsgType) E2EInterrupts()                          InputMsgType {return InputMsgType(2)}
func(InputMsgType) ThroughputAdjustment()                   InputMsgType {return InputMsgType(3)}


var MsgTypeMap = map[string]InputMsgType{
	"Invalid"                             : EInputMsgType.Invalid(),
	"CancelJob"                           : EInputMsgType.CancelJob(),
	"E2EInterrupts"                       : EInputMsgType.E2EInterrupts(),
	"ThroughputAdjustment"                : EInputMsgType.ThroughputAdjustment(),
}

type LcmMsgType struct {
	TimeStamp time.Time `json:"TimeStamp"`
	MsgType   string    `json:"MessageType"`
	Value     string    `json:"Value"`
}