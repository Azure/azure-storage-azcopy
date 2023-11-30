//go:build go1.18
// +build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package common

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

const credNamePSContext = "PSContextCredential"

type PSTokenProvider func(ctx context.Context, resource string, tenant string) ([]byte, error)
func validTenantID(tenantID string) bool {
	match, err := regexp.MatchString("^[0-9a-zA-Z-.]+$", tenantID)
	if err != nil {
		return false
	}
	return match
}

func resolveTenant(defaultTenant, specified, credName string, additionalTenants []string) (string, error) {
	if specified == "" || specified == defaultTenant {
		return defaultTenant, nil
	}
	if defaultTenant == "adfs" {
		return "", errors.New("ADFS doesn't support tenants")
	}
	if !validTenantID(specified) {
		return "", errors.New("Invalid tenant")
	}
	for _, t := range additionalTenants {
		if t == "*" || t == specified {
			return specified, nil
		}
	}
	return "", fmt.Errorf(`%s isn't configured to acquire tokens for tenant %q. To enable acquiring tokens for this tenant add it to the AdditionallyAllowedTenants on the credential options, or add "*" to allow acquiring tokens for any tenant`, credName, specified)
}
// PowershellContextCredentialOptions contains optional parameters for AzureDeveloperCLICredential.
type PowershellContextCredentialOptions struct {
	// TenantID identifies the tenant the credential should authenticate in. Defaults to the azd environment,
	// which is the tenant of the selected Azure subscription.
	TenantID string

	tokenProvider PSTokenProvider
}

// PowershellContextCredential authenticates as the identity logged in to the [Azure Developer CLI].
//
// [Azure Developer CLI]: https://learn.microsoft.com/azure/developer/azure-developer-cli/overview
type PowershellContextCredential struct {
	mu   *sync.Mutex
	opts PowershellContextCredentialOptions
}

// NewPowershellContextCredential constructs an AzureDeveloperCLICredential. Pass nil to accept default options.
func NewPowershellContextCredential(options *PowershellContextCredentialOptions) (*PowershellContextCredential, error) {
	cp := PowershellContextCredentialOptions{}
	if options != nil {
		cp = *options
	}
	if cp.TenantID != "" && !validTenantID(cp.TenantID) {
		return nil, errors.New("invalid tenant id")
	}
	if cp.tokenProvider == nil {
		cp.tokenProvider = defaultAzdTokenProvider
	}
	return &PowershellContextCredential{mu: &sync.Mutex{}, opts: cp}, nil
}

// GetToken requests a token from the Azure Developer CLI. This credential doesn't cache tokens, so every call invokes azd.
// This method is called automatically by Azure SDK clients.
func (c *PowershellContextCredential) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	at := azcore.AccessToken{}
	if len(opts.Scopes) != 1 {
		return at, errors.New(credNamePSContext + ": GetToken() exactly one scope")
	}

	tenant, err := resolveTenant(c.opts.TenantID, opts.TenantID, credNamePSContext, nil)
	if err != nil {
		return at, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := c.opts.tokenProvider(ctx, opts.Scopes[0], tenant)
	if err == nil {
		at, err = c.createAccessToken(b)
	}
	if err != nil {
		return at, err
	}
	//msg := fmt.Sprintf("%s.GetToken() acquired a token for scope %q", credNamePSContext, strings.Join(opts.Scopes, ", "))
	return at, nil
}

// We ignore resource because PS does not support all Resources. Disk scope is not supported
// and we are here only with Storage scope
var defaultAzdTokenProvider PSTokenProvider = func(ctx context.Context, _ string, tenantID string) ([]byte, error) {
	// set a default timeout for this authentication iff the application hasn't done so already
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(ctx, 10 * time.Minute)
		defer cancel()
	}

	r := regexp.MustCompile(`\{"token".*"expiresOn".*\}`)

	if tenantID != "" {
		tenantID += " -TenantId " + tenantID
	}
	cmd := `$token = Get-AzAccessToken -ResourceUrl https://storage.azure.com` + tenantID + ";"
	cmd += `$output = -join('{"token":','"',$token.Token,'"', ',"expiresOn":', '"',$token.ExpiresOn.ToString("yyyy-MM-ddTHH:mm:ss.fffK"),'"',"}");`
	cmd += "echo $output"

	cliCmd := exec.CommandContext(ctx, "powershell", cmd)
	cliCmd.Env = os.Environ()
	var stderr bytes.Buffer
	cliCmd.Stderr = &stderr

	output, err := cliCmd.Output()
	if err != nil {
		msg := stderr.String()
		if msg == "" {
			msg = err.Error()
		}
		return nil, errors.New(credNamePSContext + msg)
	}

	output = []byte(r.FindString(string(output)))
	if string(output) == "" {
		return nil, errors.New(credNamePSContext + "Invalid output while retrving token")
	}
	return output, nil
}

func (c *PowershellContextCredential) createAccessToken(tk []byte) (azcore.AccessToken, error) {
	t := struct {
		AccessToken string `json:"token"`
		ExpiresOn   string `json:"expiresOn"`
	}{}

	err := json.Unmarshal(tk, &t)
	if err != nil {
		return azcore.AccessToken{}, errors.New(err.Error())
	}
	
	parseErr := "error parsing token expiration time %q: %v"
	exp, err := time.Parse(time.RFC3339, t.ExpiresOn)
	if err != nil {
		return azcore.AccessToken{}, fmt.Errorf(parseErr, t.ExpiresOn, err)
	}
	return azcore.AccessToken{
		ExpiresOn: exp.UTC(),
		Token:     t.AccessToken,
	}, nil
}

var _ azcore.TokenCredential = (*PowershellContextCredential)(nil)