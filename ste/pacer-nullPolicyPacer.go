package ste

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"net/http"
	"sync/atomic"
)

type NullPolicyPacer struct {
	processedBytes common.AtomicNumeric[uint64]
}

func NewNullPolicyPacer() RequestPolicyPacer {
	return &NullPolicyPacer{
		processedBytes: &atomic.Uint64{},
	}
}

func (n *NullPolicyPacer) GetPacedRequestBody(body io.ReadSeekCloser, contentLength uint64) io.ReadSeekCloser {
	return body
}

func (n *NullPolicyPacer) GetPacedResponseBody(body io.ReadCloser, contentLength uint64) io.ReadCloser {
	return body
}

func (n *NullPolicyPacer) UpdateTargetBytesPerSecond(bytesPerSecond uint64) {}

func (n *NullPolicyPacer) ProcessBytes(u uint64) {
	n.processedBytes.Add(u)
}

func (n *NullPolicyPacer) UndoBytes(u uint64) {
	common.AtomicSubtract(n.processedBytes, u)
}

func (n *NullPolicyPacer) ReturnBytes(u uint64) {}

func (n *NullPolicyPacer) GetPolicy() policy.Policy {
	return &nullPacerPolicy{}
}

func (n *NullPolicyPacer) GetTotalTraffic() uint64 {
	return n.processedBytes.Load()
}

func (n *NullPolicyPacer) Cleanup() {}

type nullPacerPolicy struct {
	parent *NullPolicyPacer
}

func (n nullPacerPolicy) Do(req *policy.Request) (*http.Response, error) {
	reqBytes := uint64(max(req.Raw().ContentLength, 0))
	n.parent.ProcessBytes(reqBytes)

	resp, err := req.Next()

	if resp != nil {
		n.parent.ProcessBytes(uint64(max(resp.ContentLength, 0)))
	} else if err == nil {
		n.parent.ReturnBytes(reqBytes)
	}

	return resp, err
}
