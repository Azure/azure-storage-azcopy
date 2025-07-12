package ste

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"io"
)

type RequestPolicyPacer interface {
	GetPacedRequestBody(body io.ReadSeekCloser) (io.ReadSeekCloser, error)
	GetPacedResponseBody(body io.ReadCloser, contentLength uint64) (io.ReadCloser, error)
	UpdateTargetBytesPerSecond(bytesPerSecond uint64)
	ProcessBytes(uint64)
	ReturnBytes(uint64)
	GetPolicy() policy.Policy
	GetTotalTraffic() uint64
}

type PolicyPacerBody interface {
	io.ReadCloser
	Deallocate()
	AwaitFlight()
}

type PolicyPacerRequestBody interface {
	PolicyPacerBody
	io.Seeker
	DeallocateResponse()
}
