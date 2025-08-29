package ste

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

/*
RESPONSE Status: 401 Server failed to authenticate the request. Please refer to the information in the www-authenticate header.
Www-Authenticate: Bearer authorization_uri=https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/authorize resource_id=https://storage.azure.com
X-Ms-Error-Code: InvalidAuthenticationInfo
<?xml version="1.0" encoding="utf-8"?> <Error><Code>InvalidAuthenticationInfo</Code><Message>Server failed to authenticate the request. Please refer to the information in the www-authenticate header. </Message><AuthenticationErrorDetail>Lifetime validation failed. The token is expired.</AuthenticationErrorDetail>
*/

type destReauthPolicy struct {
	cred *common.ScopedAuthenticator
}

var reauthLock *sync.Cond = sync.NewCond(&sync.Mutex{})

func NewDestReauthPolicy(cred *common.ScopedAuthenticator) policy.Policy {
	return &destReauthPolicy{cred}
}

type destReauthDebug string

var (
	destReauthDebugExecuted                       destReauthDebug = "executed"
	destReauthDebugNoPrompt                       destReauthDebug = "destReauthNoPrompt"
	destReauthDebugCause                          destReauthDebug = "destReauthCause"
	destReauthDebugCauseAuthenticationRequired    destReauthDebug = "AuthenticationRequiredError"
	destReauthDebugCauseInvalidAuthenticationInfo destReauthDebug = "InvalidAuthenticationInfoError"
)

func (d *destReauthPolicy) Do(req *policy.Request) (*http.Response, error) {
retry:
	ctx := req.Raw().Context()
	debugCtx := context.WithValue(ctx, destReauthDebugExecuted, true)

	clone := req.Clone(ctx)
	resp, err := clone.Next() // Initially attempt the request.

	// We do a nil response check in case of timeouts and network failures
	if resp != nil && (err != nil || resp.StatusCode != http.StatusOK) { // But, if we get back an error...
		var authReq *azidentity.AuthenticationRequiredError
		var respErr = &azcore.ResponseError{}

		reauth := false

		switch { // Is it an error we can resolve by re-authing?
		case errors.As(err, &authReq):
			reauth = true
			debugCtx = context.WithValue(debugCtx, destReauthDebugCause, destReauthDebugCauseAuthenticationRequired)
		case resp != nil && resp.StatusCode == http.StatusUnauthorized:
			errors.As(runtime.NewResponseError(resp), &respErr)
			reauth = err == nil &&
				bloberror.HasCode(respErr, bloberror.InvalidAuthenticationInfo) &&
				len(respErr.RawResponse.Header.Values("WWW-Authenticate")) != 0
			if reauth {
				debugCtx = context.WithValue(debugCtx, destReauthDebugCause, destReauthDebugCauseInvalidAuthenticationInfo)
			}
		}

		if reauth { // If it is, pull the lock if we can, reauth
			m := reauthLock.L.(*sync.Mutex)

			if m.TryLock() { // Fetch the lock and try until we get auth.
				for {
					if ctx.Value(destReauthDebugNoPrompt) == nil {
						_ = common.GetLifecycleMgr().Prompt("Authentication is required to continue the job. Reauthorize and continue?", common.PromptDetails{
							PromptType: common.EPromptType.Reauth(),
							ResponseOptions: []common.ResponseOption{
								common.EResponseOption.Yes(),
							},
						})
					}

					_, err = d.cred.Authenticate(debugCtx, &policy.TokenRequestOptions{
						Scopes: []string{},
					})

					// I (Adele Reed) was initially worried about every case
					// Thinking about it further, the worst case is that the job ends automatically, or when the user asks it to end.
					// To avoid having to handle every error, we'll catch the cancel case as a way to exit the routine, but otherwise
					// we will let it happen, and just retry.
					if err == nil {
						break
					} else {
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							select {
							case <-ctx.Done(): // If it was us, exit like asked.
								return nil, err // If it was us, that's legitimately important.
							default: // If it was them, we don't care.
							}
						} else {
							common.GetLifecycleMgr().Info(fmt.Sprintf("Authentication failed, awaiting input to continue: %s", err))
						}

						time.Sleep(time.Second * 5)
					}
				}

				m.Unlock()
				reauthLock.Broadcast()
			} else { // Otherwise, wait for a signal that we can try again.
				reauthLock.Wait()
			}

			// Try the request once more
			goto retry
		} // If it wasn't, we won't retry, and we'll simply return the error.
	}

	return resp, err
}
