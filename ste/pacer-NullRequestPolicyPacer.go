package ste

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"io"
)

type NullRequestPolicyPacer struct {
}

func (n NullRequestPolicyPacer) GetPacedBody(body io.ReadSeekCloser) (io.ReadSeekCloser, error) {
	return body, nil
}

func (n NullRequestPolicyPacer) ReturnBytes(returned uint64) {
	panic("implement me")
}

func (n NullRequestPolicyPacer) GetPolicy() policy.Policy {
	//TODO implement me
	panic("implement me")
}
