package ste

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"io"
)

type RequestPolicyPacer interface {
	GetPacedRequestBody(body io.ReadSeekCloser, contentLength uint64) io.ReadSeekCloser
	GetPacedResponseBody(body io.ReadCloser, contentLength uint64) io.ReadCloser
	UpdateTargetBytesPerSecond(bytesPerSecond uint64)

	ProcessBytes(uint64)
	UndoBytes(uint64)

	GetPolicy() policy.Policy
	GetTotalTraffic() uint64
	Cleanup()
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
