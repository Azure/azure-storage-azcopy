package common

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/JeffreyRichter/enum/enum"
)

// //////////////////////////////////////////////////////////////
type LCMMsgType uint16

var ELCMMsgType LCMMsgType

func (LCMMsgType) Invalid() LCMMsgType               { return LCMMsgType(0) }
func (LCMMsgType) CancelJob() LCMMsgType             { return LCMMsgType(1) }
func (LCMMsgType) E2EInterrupts() LCMMsgType         { return LCMMsgType(2) }
func (LCMMsgType) PerformanceAdjustment() LCMMsgType { return LCMMsgType(3) }

func (m *LCMMsgType) Parse(s string) error {
	val, err := enum.Parse(reflect.TypeOf(m), s, true)
	if err == nil {
		*m = val.(LCMMsgType)
	}
	return err
}

func (m *LCMMsgType) String() string {
	return enum.StringInt(m, reflect.TypeOf(m))
}

type LCMMsgReq struct {
	TimeStamp time.Time `json:"TimeStamp"`
	MsgType   string    `json:"RequestType"`
	Value     string    `json:"Value"`
}

type LCMMsgResp struct {
	TimeStamp time.Time    `json:"TimeStamp"`
	MsgType   string       `json:"ResponseType"`
	Value     fmt.Stringer `json:"Value"`
	Err       error        `json:"-"`
}

type LCMMsg struct {
	Req      *LCMMsgReq
	Resp     *LCMMsgResp
	RespChan chan bool
}

func NewLCMMsg() *LCMMsg {
	return &LCMMsg{RespChan: make(chan bool)}
}

func (m *LCMMsg) SetRequest(req *LCMMsgReq) {
	m.Req = req
}

func (m *LCMMsg) SetResponse(resp *LCMMsgResp) {
	m.Resp = resp
}

func (m *LCMMsg) Reply() {
	m.RespChan <- true
}

////////////////////////////////////////////////////////////////////////////////////

/* PerfAdjustment message. */
type PerfAdjustmentReq struct {
	Throughput int64 `json:"cap-mbps,string"`
}

type PerfAdjustmentResp struct {
	Status              bool      `json:"status"`
	AdjustedThroughPut  int64     `json:"cap-mbps"`
	NextAdjustmentAfter time.Time `json:"NextAdjustmentAfter"`
	Err                 string    `json:"error"`
}

func (p PerfAdjustmentResp) String() string {
	ret := ""
	if p.Status {
		ret = fmt.Sprintf("Successfully adjust throughput to %d Mbps.", p.AdjustedThroughPut)
	} else {
		ret = fmt.Sprintf("Failed to adjust throughput. %s", p.Err)
	}

	return ret
}

func (p PerfAdjustmentResp) Json() string {
	r, e := json.Marshal(p)
	PanicIfErr(e)
	return string(r)
}
