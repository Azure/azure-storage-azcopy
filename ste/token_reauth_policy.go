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
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
)

/*
===== TARGET DEST FAILURE =====
RESPONSE Status: 401 Server failed to authenticate the request. Please refer to the information in the www-authenticate header.
Www-Authenticate: Bearer authorization_uri=https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/authorize resource_id=https://storage.azure.com
X-Ms-Error-Code: InvalidAuthenticationInfo
<?xml version="1.0" encoding="utf-8"?> <Error><Code>InvalidAuthenticationInfo</Code><Message>Server failed to authenticate the request. Please refer to the information in the www-authenticate header. </Message><AuthenticationErrorDetail>Lifetime validation failed. The token is expired.</AuthenticationErrorDetail>

===== TARGET SOURCE FAILURE =====
RESPONSE Status: 401 Server failed to authenticate the request. Please refer to the information in the www-authenticate header.
Content-Length: 568
Content-Type: application/xml
Date: Thu, 02 Jul 2026 21:07:46 GMT
Server: Windows-Azure-Blob/1.0 Microsoft-HTTPAPI/2.0
X-Ms-Copy-Source-Error-Code: InvalidAuthenticationInfo
X-Ms-Copy-Source-Status-Code: 401
X-Ms-Error-Code: CannotVerifyCopySource
X-Ms-Request-Id: 36dc36e4-101e-00c8-6766-0a3888000000
X-Ms-Version: 2025-01-05
Response Details: <Code>CannotVerifyCopySource</Code><Message>Server failed to authenticate the request. Please refer to the information in the www-authenticate header. </Message><CopySourceStatusCode>401</CopySourceStatusCode><CopySourceErrorCode>InvalidAuthenticationInfo</CopySourceErrorCode><CopySourceErrorMessage>Server failed to authenticate the request. Please refer to the information in the www-authenticate header.</CopySourceErrorMessage>
*/

type tokenReauthPolicy struct {
	srcToken    cred.ScopedAuthenticator
	targetToken cred.ScopedAuthenticator // targetToken is also for the destination, but has a more fitting name, for something like delete.
}

var reauthLock = sync.NewCond(&sync.Mutex{})

type NewTokenReauthPolicyOptions struct {
	srcToken cred.ScopedAuthenticator
}

func NewTokenReauthPolicy(targetToken cred.ScopedAuthenticator, opts ...NewTokenReauthPolicyOptions) policy.Policy {
	opt := ternary.FirstOrZero(opts)

	return &tokenReauthPolicy{
		targetToken: targetToken,
		srcToken:    opt.srcToken,
	}
}

type tokenReauthDebug string

var (
	tokenReauthDebugExecuted tokenReauthDebug = "executed"
	tokenReauthNoPrompt      tokenReauthDebug = "destReauthNoPrompt"

	tokenReauthDebugCauseAuthenticationRequired    tokenReauthDebug = "AuthenticationRequiredError"
	tokenReauthDebugCauseInvalidAuthenticationInfo tokenReauthDebug = "InvalidAuthenticationInfoError"

	destReauthDebugCause tokenReauthDebug = "destReauthCause"
	srcReauthDebugCause  tokenReauthDebug = "srcReauthCause"
)

func (d *tokenReauthPolicy) Do(req *policy.Request) (*http.Response, error) {
	for {
		ctx := req.Raw().Context()
		debugCtx := context.WithValue(ctx, tokenReauthDebugExecuted, true)

		clone := req.Clone(ctx)
		resp, err := clone.Next() // Initially attempt the request.

		if err != nil || (resp.StatusCode/100) == 4 {
			if d.checkDestAuthFailure(err, resp, &debugCtx) {
				if reauthErr := d.reauthToken(d.targetToken, debugCtx); reauthErr != nil {
					return resp, reauthErr
				}

				continue // retry after reauth
			}

			if d.checkSourceAuthFailure(err, resp, &debugCtx) {
				if reauthErr := d.reauthToken(d.srcToken, debugCtx); reauthErr != nil {
					return resp, reauthErr
				}

				continue // retry after reauth
			}
		}

		return resp, err
	}
}

func (d *tokenReauthPolicy) checkDestAuthFailure(err error, resp *http.Response, debugCtx *context.Context) (needsReauth bool) {
	var authReq *azidentity.AuthenticationRequiredError
	var respErr = &azcore.ResponseError{}

	if d.targetToken == nil {
		return false
	}

	if debugCtx == nil {
		ctx := context.Background()
		debugCtx = &ctx
	}

	switch { // Is it an error we can resolve by re-authing?
	case errors.As(err, &authReq):
		needsReauth = true
		*debugCtx = context.WithValue(*debugCtx, destReauthDebugCause, tokenReauthDebugCauseAuthenticationRequired)
	case resp != nil && resp.StatusCode == http.StatusUnauthorized:
		errors.As(runtime.NewResponseError(resp), &respErr)

		needsReauth = err == nil &&
			bloberror.HasCode(respErr, bloberror.InvalidAuthenticationInfo) &&
			len(respErr.RawResponse.Header.Values("WWW-Authenticate")) != 0
		if needsReauth {
			*debugCtx = context.WithValue(*debugCtx, srcReauthDebugCause, tokenReauthDebugCauseInvalidAuthenticationInfo)
		}
	}

	return
}

func (d *tokenReauthPolicy) checkSourceAuthFailure(err error, resp *http.Response, debugCtx *context.Context) (needsReauth bool) {
	var authReq *sourceAuthenticationRequiredError
	var respErr = &azcore.ResponseError{}

	if d.srcToken == nil {
		return false
	}

	if debugCtx == nil {
		ctx := context.Background()
		debugCtx = &ctx
	}

	switch { // Is it an error we can resolve by re-authing?
	case errors.As(err, &authReq):
		needsReauth = true
		*debugCtx = context.WithValue(*debugCtx, srcReauthDebugCause, tokenReauthDebugCauseAuthenticationRequired)
	case resp != nil && resp.StatusCode == http.StatusUnauthorized:
		errors.As(runtime.NewResponseError(resp), &respErr)

		needsReauth = err == nil && bloberror.HasCode(respErr, bloberror.CannotVerifyCopySource)

		if needsReauth {
			*debugCtx = context.WithValue(*debugCtx, destReauthDebugCause, tokenReauthDebugCauseInvalidAuthenticationInfo)
		}
	}

	return
}

func (d *tokenReauthPolicy) reauthToken(c cred.ScopedAuthenticator, ctx context.Context) error {
	m := reauthLock.L.(*sync.Mutex)

	if m.TryLock() { // Fetch the lock and try until we get auth.
		for {
			if ctx.Value(tokenReauthNoPrompt) == nil {
				_ = common.GetLifecycleMgr().Prompt("Authentication is required to continue the job. Reauthorize and continue?", common.PromptDetails{
					PromptType: common.EPromptType.Reauth(),
					ResponseOptions: []common.ResponseOption{
						common.EResponseOption.Yes(),
					},
				})
			}

			_, err := c.Authenticate(ctx, nil) // value is ignored, so nil is passed in

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
						return err // If it was us, that's legitimately important.
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

	return nil
}
