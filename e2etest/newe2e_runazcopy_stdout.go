package e2etest

import (
	"encoding/json"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var _ AzCopyStdout = &AzCopyParsedStdout{}
var _ AzCopyStdout = &AzCopyParsedListStdout{}
var _ AzCopyStdout = &AzCopyParsedCopySyncRemoveStdout{}
var _ AzCopyStdout = &AzCopyParsedDryrunStdout{}
var _ AzCopyStdout = &AzCopyParsedJobsListStdout{}
var _ AzCopyStdout = &AzCopyParsedJobsShowStdout{}

// ManySubscriberChannel is intended to reproduce the effects of .NET's events.
// This allows us to *partially* answer the question of how we want to handle testing of prompting in the New E2E framework.
// Assuming an async version of RunAzCopy eventually exists, one could hook into the relevant AzCopyParsedStdout-extending struct
// and reply whenever a prompt is given.
type ManySubscriberChannel[T any] struct {
	subscribers map[chan<- T]bool
}

func (m *ManySubscriberChannel[T]) Subscribe(target chan<- T) {
	if m.subscribers == nil {
		m.subscribers = make(map[chan<- T]bool)
	}

	m.subscribers[target] = true
}

func (m *ManySubscriberChannel[T]) SubscribeFunc(target func(T)) (handle chan<- T) {
	targetChannel := make(chan T)

	go func() {
		for {
			data, ok := <-targetChannel
			if !ok {
				break
			}

			target(data)
		}
	}()

	m.Subscribe(targetChannel)
	return targetChannel
}

// Unsubscribe will close the target channel, in addition to removing it from this channel's subscribers.
func (m *ManySubscriberChannel[T]) Unsubscribe(target chan<- T) {
	if m.subscribers == nil {
		return
	}

	delete(m.subscribers, target)
	close(target)
}

func (m *ManySubscriberChannel[T]) Message(data T) {
	for k := range m.subscribers {
		k <- data
	}
}

// AzCopyParsedStdout is still a semi-raw stdout struct.
type AzCopyParsedStdout struct {
	Messages     []cmd.JsonOutputTemplate
	OnParsedLine ManySubscriberChannel[cmd.JsonOutputTemplate]
}

func (a *AzCopyParsedStdout) RawStdout() []string {
	out := make([]string, len(a.Messages))

	for k, v := range a.Messages {
		buf, _ := json.Marshal(v)

		out[k] = string(buf)
	}

	return out
}

func (a *AzCopyParsedStdout) Write(p []byte) (n int, err error) {
	str := string(p)
	lines := strings.Split(strings.TrimSuffix(str, "\n"), "\n")
	n = len(p)

	for _, v := range lines {
		// Instead of failing, skip WARN messages since they will fail processing due to being invalid JSON.
		if strings.HasPrefix(v, "WARN") {
			continue
		}
		var out cmd.JsonOutputTemplate
		err = json.Unmarshal([]byte(v), &out)
		if err != nil {
			return
		}

		a.OnParsedLine.Message(out)
		a.Messages = append(a.Messages, out)
	}

	return
}

func (a *AzCopyParsedStdout) String() string {
	return strings.Join(a.RawStdout(), "\n")
}

type AzCopyParsedListStdout struct {
	AzCopyParsedStdout
	listenChan chan<- cmd.JsonOutputTemplate

	Items   map[AzCopyOutputKey]cmd.AzCopyListObject
	Summary cmd.AzCopyListSummary
}

func (a *AzCopyParsedListStdout) InsertObject(obj cmd.AzCopyListObject) {
	if a.Items == nil {
		a.Items = make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	}

	a.Items[AzCopyOutputKey{
		Path:      obj.Path,
		VersionId: obj.VersionId,
	}] = obj
}

func (a *AzCopyParsedListStdout) Write(p []byte) (n int, err error) {
	if a.listenChan == nil {
		a.listenChan = a.OnParsedLine.SubscribeFunc(func(line cmd.JsonOutputTemplate) {
			switch line.MessageType {
			case "ListObject":
				var object cmd.AzCopyListObject
				err = json.Unmarshal([]byte(line.MessageContent), &object)
				if err != nil {
					return
				}

				a.InsertObject(object)

			case "ListSummary":
				err = json.Unmarshal([]byte(line.MessageContent), &a.Summary)
				if err != nil {
					return
				}
			}
		})
	}

	return a.AzCopyParsedStdout.Write(p)
}

type AzCopyParsedCopySyncRemoveStdout struct {
	AzCopyParsedStdout
	listenChan chan<- cmd.JsonOutputTemplate

	JobPlanFolder string
	LogFolder     string

	InitMsg     cmd.InitMsgJsonTemplate
	FinalStatus common.ListJobSummaryResponse
}

func (a *AzCopyParsedCopySyncRemoveStdout) Write(p []byte) (n int, err error) {
	if a.listenChan == nil {
		a.listenChan = a.OnParsedLine.SubscribeFunc(func(line cmd.JsonOutputTemplate) {
			switch line.MessageType {
			case cmd.EOutputMessageType.EndOfJob().String():
				_ = json.Unmarshal([]byte(line.MessageContent), &a.FinalStatus)
			case cmd.EOutputMessageType.Init().String():
				_ = json.Unmarshal([]byte(line.MessageContent), &a.InitMsg)
			}
		})
	}

	return a.AzCopyParsedStdout.Write(p)
}

type AzCopyParsedDryrunStdout struct {
	AzCopyRawStdout

	fromTo common.FromTo // fallback for text output

	listenChan chan<- cmd.DryrunTransfer

	Transfers []cmd.DryrunTransfer
	Raw       map[string]bool
	JsonMode  bool
}

func (d *AzCopyParsedDryrunStdout) Write(p []byte) (n int, err error) {
	lines := strings.Split(string(p), "\n")
	for _, str := range lines {
		if !d.JsonMode && strings.HasPrefix(str, "DRYRUN: ") {
			if strings.HasPrefix(str, "DRYRUN: warn") {
				continue
			}

			d.Raw[str] = true
		} else {
			var out cmd.JsonOutputTemplate
			err = json.Unmarshal([]byte(str), &out)
			if err != nil {
				continue
			}

			if out.MessageType != cmd.EOutputMessageType.Dryrun().String() {
				continue
			}

			var tx cmd.DryrunTransfer
			err = json.Unmarshal([]byte(out.MessageContent), &tx)
			if err != nil {
				continue
			}

			d.Transfers = append(d.Transfers, tx)
		}
	}

	return d.AzCopyRawStdout.Write(p)
}

type AzCopyParsedJobsListStdout struct {
	AzCopyParsedStdout
	listenChan chan<- cmd.JsonOutputTemplate
	JobsCount  int
	Jobs       []common.JobIDDetails
}

func (a *AzCopyParsedJobsListStdout) Write(p []byte) (n int, err error) {
	if a.listenChan == nil {
		a.listenChan = a.OnParsedLine.SubscribeFunc(func(line cmd.JsonOutputTemplate) {
			if line.MessageType == cmd.EOutputMessageType.EndOfJob().String() {
				var tx common.ListJobsResponse
				err = json.Unmarshal([]byte(line.MessageContent), &tx)
				if err != nil {
					return
				}

				a.JobsCount = len(tx.JobIDDetails)
				a.Jobs = tx.JobIDDetails
			}
		})
	}
	return a.AzCopyParsedStdout.Write(p)
}

type AzCopyParsedLoginStatusStdout struct {
	AzCopyParsedStdout
	listenChan chan<- cmd.JsonOutputTemplate
	status     cmd.LoginStatusOutput
}

func (a *AzCopyParsedLoginStatusStdout) Write(p []byte) (n int, err error) {
	if a.listenChan == nil {
		a.listenChan = a.OnParsedLine.SubscribeFunc(func(line cmd.JsonOutputTemplate) {
			if line.MessageType == cmd.EOutputMessageType.LoginStatusInfo().String() {
				out := &cmd.LoginStatusOutput{}
				err = json.Unmarshal([]byte(line.MessageContent), out)
				if err != nil {
					return
				}

				a.status = *out
			}
		})
	}
	return a.AzCopyParsedStdout.Write(p)
}

var _ AzCopyStdout = &AzCopyInteractiveStdout{}

// AzCopyInteractiveStdout is still a semi-raw stdout struct.
type AzCopyInteractiveStdout struct {
	Messages []string
	asserter Asserter
}

// NewInteractiveWriter creates a new InteractiveWriter instance.
func NewAzCopyInteractiveStdout(a Asserter) *AzCopyInteractiveStdout {
	return &AzCopyInteractiveStdout{
		asserter: a,
	}
}

func (a *AzCopyInteractiveStdout) RawStdout() []string {
	return a.Messages
}

func (a *AzCopyInteractiveStdout) Write(p []byte) (n int, err error) {
	str := string(p)
	lines := strings.Split(strings.TrimSuffix(str, "\n"), "\n")
	n = len(p)

	for _, v := range lines {
		a.Messages = append(a.Messages, v)
		a.asserter.Log(v)
	}

	return
}

func (a *AzCopyInteractiveStdout) String() string {
	return strings.Join(a.RawStdout(), "\n")
}

type AzCopyParsedJobsShowStdout struct {
	AzCopyParsedStdout
	listenChan chan<- cmd.JsonOutputTemplate
	transfers  common.ListJobTransfersResponse
	summary    common.ListJobSummaryResponse
}

func (a *AzCopyParsedJobsShowStdout) Write(p []byte) (n int, err error) {
	if a.listenChan == nil {
		a.listenChan = a.OnParsedLine.SubscribeFunc(func(line cmd.JsonOutputTemplate) {
			if line.MessageType == cmd.EOutputMessageType.ListJobTransfers().String() {
				var tx common.ListJobTransfersResponse
				err = json.Unmarshal([]byte(line.MessageContent), &tx)
				if err != nil {
					return
				}
				a.transfers = tx
			} else if line.MessageType == cmd.EOutputMessageType.GetJobSummary().String() {
				var summary common.ListJobSummaryResponse
				err = json.Unmarshal([]byte(line.MessageContent), &summary)
				if err != nil {
					return
				}
				a.summary = summary
			}
		})
	}
	return a.AzCopyParsedStdout.Write(p)
}
