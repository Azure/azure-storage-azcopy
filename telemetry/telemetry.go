// As part of the White Glove White Glove project,
// this is a telemetry package to support posting summary and cancellation events
// to a given webhook endpoint with a reference project id in the shape of an event grid
// custom event.

package telemetry

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Azure/azure-storage-azcopy/common"
)

var (
	// ReportingAPIURI the Webhook URI
	ReportingAPIURI string
	// ProjectReferenceID the project reference ID
	ProjectReferenceID string
)

type telemetry struct {
	JobID     string      `json:"jobID"`
	ProjectID string      `json:"projectId"`
	Telemetry interface{} `json:"telemetry"`
}

type reportEvent struct {
	EventType  string      `json:"eventType"`
	EventTopic string      `json:"eventTopic"`
	Data       interface{} `json:"data"`
	EventTime  time.Time   `json:"eventTime"`
}

func getJSON(t interface{}) string {
	json, err := json.Marshal(t)
	common.PanicIfErr(err)
	return string(json)
}

func newReportEvent(eventType string, data interface{}) *reportEvent {
	return &reportEvent{
		EventType:  eventType,
		EventTopic: "",
		Data:       data,
		EventTime:  time.Now(),
	}
}

func postEvent(json string) {
	var jsonStr = []byte(json)
	_, err := http.Post(ReportingAPIURI, "application/json", bytes.NewBuffer(jsonStr))
	common.PanicIfErr(err)
}

func appendToFile(content string) {
	f, err := os.OpenFile("./telemetry.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		log.Println(err)
	}
}

// RaiseCancellationEvent raises a cancellation event
func RaiseCancellationEvent(jobID string) {
	if ReportingAPIURI != "" {
		telemetry := telemetry{
			JobID:     jobID,
			ProjectID: ProjectReferenceID,
		}
		evt := newReportEvent("job-cancellation-event", telemetry)
		json := getJSON(evt)
		postEvent(json)
	}
}

// RaiseSummaryEvent raises a telemetry event
func RaiseSummaryEvent(summary common.ListJobSummaryResponse) {
	if ReportingAPIURI != "" && ProjectReferenceID != "" {
		telemetry := telemetry{
			JobID:     summary.JobID.String(),
			ProjectID: ProjectReferenceID,
			Telemetry: summary,
		}
		evt := newReportEvent("job-telemetry-event", telemetry)
		json := getJSON(evt)
		postEvent(json)
	}
}

// RaiseLogingEvent raises a telemetry event
func RaiseLogingEvent(jobID, msg string) {
	if ReportingAPIURI != "" && ProjectReferenceID != "" {
		telemetry := telemetry{
			JobID:     jobID,
			ProjectID: ProjectReferenceID,
			Telemetry: msg,
		}
		evt := newReportEvent("job-telemetry-event", telemetry)
		json := getJSON(evt)
		postEvent(json)
	}
}
