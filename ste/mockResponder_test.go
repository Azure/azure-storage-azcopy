package ste

import (
	"bytes"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"io"
	"net/http"
)

type MockResponder struct {
	resp http.Response
	body []byte
}

func (m MockResponder) Do(req *policy.Request) (*http.Response, error) {
	r := m.resp
	r.Header = m.resp.Header.Clone()
	r.Body = io.NopCloser(bytes.NewReader(m.body))

	r.Request = req.Raw()
	return &r, nil
}
