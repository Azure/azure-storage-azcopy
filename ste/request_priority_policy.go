package ste

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/go-autorest/autorest/date"
	"net/http"
	"strconv"
	"sync"
)

var (
	requestPriorityDate = func() date.Date {
		out, err := date.ParseDate(requestPriorityDateString) // todo: might not be correct date
		if err != nil {
			panic("request priority service date didn't parse: " + err.Error())
		}

		return out
	}()

	requestPriorityLogOnce = &sync.Once{}

	// GlobalRequestPriority at risk of becoming yet another angle in which temporary solutions become very permanent
	// attempts to avoid the current issue of context usage. Context usage will get fixed up in a follow-up PR, but
	// to save me (Adele) from going bonkers, I'm going to push this out with a jank solution to begin.
	GlobalRequestPriority int = -1
)

const (
	requestPriorityDateString = "2026-04-06"
	XMsRequestPriority        = "x-ms-request-priority"
)

type requestPriorityPolicy struct {
	priorityData *int
}

type requestPriorityOverride struct{}

// NewRequestPriorityPolicy creates a new requestPriorityPolicy, which should be placed before a NewVersionPolicy.
func NewRequestPriorityPolicy() policy.Policy {
	return requestPriorityPolicy{priorityData: &GlobalRequestPriority}
}

func (r requestPriorityPolicy) Do(req *policy.Request) (*http.Response, error) {
	if *r.priorityData == -1 { // -1 is the bypass value
		return req.Next()
	}

	tryParse := func(str string, target *date.Date) bool {
		var err error
		*target, err = date.ParseDate(str)
		return err == nil
	}

	var stgDate date.Date // fetch whatever service version we can get
	if strVal, OK := req.Raw().Context().Value(ServiceAPIVersionOverride).(string); OK && tryParse(strVal, &stgDate) {
	} else if strVal = req.Raw().Header.Get(XMsVersion); strVal != "" && tryParse(strVal, &stgDate) {
	}

	if stgDate.Before(requestPriorityDate.Time) {
		requestPriorityLogOnce.Do(func() {
			common.GetLifecycleMgr().Info(fmt.Sprintf(
				"x-ms-version %v is not new enough to support custom request priority versions, upgrading all requests to %v",
				stgDate.String(),
				requestPriorityDateString,
			))
		})

		// insert a fresh override key, because newer values "override" older values.
		req = req.WithContext(context.WithValue(req.Raw().Context(), ServiceAPIVersionOverride, requestPriorityDateString))
	}

	req.Raw().Header.Set(XMsRequestPriority, strconv.Itoa(*r.priorityData))

	return req.Next()
}
