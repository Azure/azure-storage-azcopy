package e2etest

import (
	"encoding/json"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"strings"
)

var _ AzCopyStdout = &AzCopyParsedStdout{}
var _ AzCopyStdout = &AzCopyParsedListStdout{}
var _ AzCopyStdout = &AzCopyParsedCopySyncRemoveStdout{}
var _ AzCopyStdout = &AzCopyParsedDryrunStdout{}

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
	Messages     []common.JsonOutputTemplate
	OnParsedLine ManySubscriberChannel[common.JsonOutputTemplate]
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
		var out common.JsonOutputTemplate
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
	listenChan chan<- common.JsonOutputTemplate

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
		a.listenChan = a.OnParsedLine.SubscribeFunc(func(line common.JsonOutputTemplate) {
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
	listenChan chan<- common.JsonOutputTemplate

	InitMsg     common.InitMsgJsonTemplate
	FinalStatus common.ListJobSummaryResponse
}

func (a *AzCopyParsedCopySyncRemoveStdout) Write(p []byte) (n int, err error) {
	if a.listenChan == nil {
		a.listenChan = a.OnParsedLine.SubscribeFunc(func(line common.JsonOutputTemplate) {
			switch line.MessageType {
			case common.EOutputMessageType.EndOfJob().String():
				_ = json.Unmarshal([]byte(line.MessageContent), &a.FinalStatus)
			case common.EOutputMessageType.Init().String():
				_ = json.Unmarshal([]byte(line.MessageContent), &a.InitMsg)
			}
		})
	}

	return a.AzCopyParsedStdout.Write(p)
}

type AzCopyParsedDryrunStdout struct {
	AzCopyParsedStdout
	listenChan chan<- common.JsonOutputTemplate

	ScheduledTransfers map[string]common.CopyTransfer
}

func (a *AzCopyParsedDryrunStdout) Write(p []byte) (n int, err error) {
	if a.listenChan == nil {
		a.listenChan = a.OnParsedLine.SubscribeFunc(func(line common.JsonOutputTemplate) {
			if line.MessageType == common.EOutputMessageType.Dryrun().String() {
				var tx common.CopyTransfer
				err = json.Unmarshal([]byte(line.MessageContent), &tx)
				if err != nil {
					return
				}

				a.ScheduledTransfers[tx.Source] = tx
			}
		})
	}

	return a.AzCopyParsedStdout.Write(p)
}
