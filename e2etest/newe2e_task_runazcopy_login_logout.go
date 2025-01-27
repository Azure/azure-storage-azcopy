package e2etest

import (
	"fmt"
	"os/exec"
	"strings"
)

var _ AzCopyStdout = &AzCopyInteractiveStdout{}

// AzCopyInteractiveStdout is still a semi-raw stdout struct.
type AzCopyInteractiveStdout struct {
	Messages []string
	asserter Asserter
}

// NewInteractiveWriter creates a new InteractiveWriter instance.
func NewAzCopyInteractiveStdout(a Asserter) *AzCopyInteractiveStdout {
	return &AzCopyInteractiveStdout{
		asserter: a,
	}
}

func (a *AzCopyInteractiveStdout) RawStdout() []string {
	return a.Messages
}

func (a *AzCopyInteractiveStdout) Write(p []byte) (n int, err error) {
	str := string(p)
	lines := strings.Split(strings.TrimSuffix(str, "\n"), "\n")
	n = len(p)

	for _, v := range lines {
		a.Messages = append(a.Messages, v)
		a.asserter.Log(v)
	}

	return
}

func (a *AzCopyInteractiveStdout) String() string {
	return strings.Join(a.RawStdout(), "\n")
}

func RunAzCopyLoginLogout(a Asserter, verb AzCopyVerb) AzCopyStdout {
	out := NewAzCopyInteractiveStdout(a)

	args := []string{GlobalConfig.AzCopyExecutableConfig.ExecutablePath, string(verb)}

	tenantId := GlobalConfig.E2EAuthConfig.StaticStgAcctInfo.StaticOAuth.TenantID
	if verb == AzCopyVerbLogin && tenantId != "" {
		args = append(args, fmt.Sprintf("--tenant-id=%s", tenantId))
	}

	command := exec.Cmd{
		Path:   GlobalConfig.AzCopyExecutableConfig.ExecutablePath,
		Args:   args,
		Stdout: out,
	}

	in, err := command.StdinPipe()
	a.NoError("get stdin pipe", err)

	err = command.Start()
	a.Assert("run command", IsNil{}, err)

	if isLaunchedByDebugger {
		beginAzCopyDebugging(in)
	}

	err = command.Wait()

	a.Assert("wait for finalize", IsNil{}, err)
	a.Assert("expected exit code", Equal{}, 0, command.ProcessState.ExitCode())

	return out
}
