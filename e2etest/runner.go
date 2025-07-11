// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package e2etest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// encapsulates the interaction with the AzCopy instance that is being tested
// the flag names should be captured here so that in case they change, only 1 place needs to be updated
type TestRunner struct {
	flags map[string]string
}

func newTestRunner() TestRunner {
	return TestRunner{flags: make(map[string]string)}
}

var isLaunchedByDebugger = func() bool {
	// gops executable must be in the path. See https://github.com/google/gops
	gopsOut, err := exec.Command("gops", strconv.Itoa(os.Getppid())).Output()
	if err == nil && strings.Contains(string(gopsOut), "\\dlv.exe") {
		// our parent process is (probably) the Delve debugger
		return true
	}
	return false
}()

func (t *TestRunner) SetAllFlags(s *scenario) {
	p := s.p
	o := s.operation

	set := func(key string, value interface{}, dflt interface{}, formats ...string) {
		if value == dflt {
			return // nothing to do. The flag is not supposed to be set
		}

		reflectVal := reflect.ValueOf(value) // check for pointer
		if reflectVal.Kind() == reflect.Pointer {
			result := reflectVal.Elem() // attempt to deref

			if result != (reflect.Value{}) && result.CanInterface() { // can we grab the underlying value?
				value = result.Interface()
			} else {
				return // nothing to use
			}
		}

		format := "%v"
		if len(formats) > 0 {
			format = formats[0]
		}

		t.flags[key] = fmt.Sprintf(format, value)
	}

	if o == eOperation.Benchmark() {
		set("mode", p.mode, "")
		set("file-count", p.fileCount, 0)
		set("size-per-file", p.sizePerFile, "")
		return
	}

	if o == eOperation.Cancel() {
		set("ignore-error-if-completed", p.ignoreErrorIfCompleted, "")
		return
	}

	// TODO: TODO: nakulkar-msft there will be many more to add here
	set("recursive", p.recursive, false)
	set("as-subdir", !p.invertedAsSubdir, true)
	set("include-path", p.includePath, "")
	set("exclude-path", p.excludePath, "")
	set("include-pattern", p.includePattern, "")
	set("exclude-pattern", p.excludePattern, "")
	set("include-after", p.includeAfter, "")
	set("include-pattern", p.includePattern, "")
	set("exclude-path", p.excludePath, "")
	set("exclude-pattern", p.excludePattern, "")
	set("cap-mbps", p.capMbps, float32(0))
	set("block-size-mb", p.blockSizeMB, float32(0))
	set("put-blob-size-mb", p.putBlobSizeMB, float32(0))
	set("s2s-detect-source-changed", p.s2sSourceChangeValidation, false)
	set("metadata", p.metadata, "")
	set("cancel-from-stdin", p.cancelFromStdin, false)
	set("preserve-smb-info", p.preserveSMBInfo, nil)
	set("preserve-smb-permissions", p.preserveSMBPermissions, false)
	set("backup", p.backupMode, false)
	set("blob-tags", p.blobTags, "")
	set("blob-type", p.blobType, "")
	set("s2s-preserve-blob-tags", p.s2sPreserveBlobTags, false)
	set("cpk-by-name", p.cpkByName, "")
	set("cpk-by-value", p.cpkByValue, false)
	set("is-object-dir", p.isObjectDir, false)
	set("debug-skip-files", strings.Join(p.debugSkipFiles, ";"), "")
	set("check-md5", p.checkMd5.String(), "FailIfDifferent")
	set("trailing-dot", p.trailingDot.String(), "Enable")
	set("force-if-read-only", p.forceIfReadOnly, false)
	set("delete-destination-file", p.deleteDestinationFile, false)

	if o == eOperation.Copy() {
		set("s2s-preserve-access-tier", p.s2sPreserveAccessTier, true)
		set("preserve-posix-properties", p.preservePOSIXProperties, "")

		switch p.symlinkHandling {
		case common.ESymlinkHandlingType.Follow():
			set("follow-symlinks", true, nil)
		case common.ESymlinkHandlingType.Preserve():
			set("preserve-symlinks", true, nil)
		}

		target := s.GetTestFiles().objectTarget
		if s.fromTo.From() == common.ELocation.Blob() && s.fs.isListOfVersions() { // Otherwise, it must be a list.
			s.a.Assert(s.fromTo.From(), equals(), common.ELocation.Blob(), "list of files can only be used in blob.")

			versions := s.GetSource().(*resourceBlobContainer).getVersions(s.a, target.objectName)
			s.a.Assert(len(versions) > 0, equals(), true, "blob was expected to have versions!")
			listOfVersions := make([]string, len(target.versions))

			for idx, val := range target.versions {
				s.a.Assert(int(val) < len(versions), equals(), true, fmt.Sprintf("Not enough versions are present! (needed version %d of %d)", val, len(versions)))
				listOfVersions[idx] = versions[val]
			}

			file, err := os.CreateTemp("", "listofversions*.json")
			defer func(file *os.File) {
				_ = file.Close()
			}(file)
			s.a.AssertNoErr(err, "create temp list of versions file")

			for _, v := range listOfVersions {
				_, err = file.WriteString(v + "\n")
				s.a.AssertNoErr(err, "write to list of versions file")
			}

			set("list-of-versions", file.Name(), "")
		}
	} else if o == eOperation.Sync() {
		set("delete-destination", p.deleteDestination.String(), "False")
		set("preserve-posix-properties", p.preservePOSIXProperties, false)
		set("compare-hash", p.compareHash.String(), "None")
		set("local-hash-storage-mode", p.hashStorageMode.String(), common.EHashStorageMode.Default().String())
		set("hash-meta-dir", p.hashStorageDir, "")
	}
}

func (t *TestRunner) SetAwaitOpenFlag() {
	t.flags["await-open"] = "true"
}

func (t *TestRunner) computeArgs() []string {
	args := make([]string, 0)
	for key, value := range t.flags {
		args = append(args, fmt.Sprintf("--%s=%s", key, value))
	}
	args = append(args, "--log-level=DEBUG")

	return append(args, "--output-type=json")
}

// execCommandWithOutput replaces Go's exec.Command().Output, but appends an extra parameter and
// breaks up the c.Run() call into its component parts. Both changes are to assist debugging
func (t *TestRunner) execDebuggableWithOutput(name string, args []string, env []string, afterStart func() string, chToStdin <-chan string) ([]byte, error) {
	debug := isLaunchedByDebugger
	if debug {
		args = append(args, "--await-continue")
	}
	c := exec.Command(name, args...)

	// add environment variables
	if env != nil {
		c.Env = env
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	stdin, err := c.StdinPipe()
	if err != nil {
		return make([]byte, 0), err
	}

	c.Stdout = &stdout
	c.Stderr = &stderr

	// instead of err := c.Run(), we do the following
	runErr := c.Start()
	if runErr == nil {
		defer func() {
			_ = c.Process.Kill() // in case we never finish c.Wait() below, and get panicked or killed
		}()

		if debug {
			beginAzCopyDebugging(stdin)
		}

		// perform a specific post-start action
		if afterStart != nil {
			msgToApp := afterStart() // perform a local action, here in the test suite, that may optionally produce a message to send to the the app
			if msgToApp != "" {
				_, _ = stdin.Write([]byte(msgToApp + "\n")) // TODO: maybe change this to use chToStdIn
			}
		}

		// allow on-going messages to stdin
		if chToStdin != nil {
			go func() {
				for {
					msg, ok := <-chToStdin
					if ok {
						_, _ = stdin.Write([]byte(msg + "\n"))
					} else {
						break
					}
				}
			}()
		}

		// wait for completion
		runErr = c.Wait()
	}

	// back to normal exec.Cmd.Output() processing
	if runErr != nil {
		if ee, ok := runErr.(*exec.ExitError); ok {
			ee.Stderr = stderr.Bytes()
		}
	}
	return stdout.Bytes(), runErr
}

func (t *TestRunner) ExecuteAzCopyCommand(operation Operation, src, dst string, needsOAuth bool, oauthMode string, needsFromTo bool, fromTo common.FromTo, afterStart func() string, chToStdin <-chan string, logDir string) (CopyOrSyncCommandResult, bool, error) {
	capLen := func(b []byte) []byte {
		if len(b) < 1024 {
			return b
		} else {
			return append(b[:1024], byte('\n'))
		}
	}

	verb := ""
	switch operation {
	case eOperation.Copy():
		verb = "copy"
	case eOperation.Sync():
		verb = "sync"
	case eOperation.Remove():
		verb = "remove"
	case eOperation.Resume():
		verb = "jobs resume"
	case eOperation.Cancel():
		verb = "cancel"
	case eOperation.Benchmark():
		verb = "bench"
	default:
		panic("unsupported operation type")
	}

	args := strings.Split(verb, " ")
	args = append(args, src)
	if operation.NeedsDst() {
		args = append(args, dst)
	}
	args = append(args, t.computeArgs()...)
	if needsFromTo {
		args = append(args, "--from-to="+fromTo.String())
	}

	// pass along existing environment variables (because $HOME doesn't come along if we just use the OAuth vars, that can be troublesome!)
	env := make([]string, len(os.Environ()))
	copy(env, os.Environ())

	if needsOAuth {
		switch strings.ToLower(oauthMode) {
		case common.EAutoLoginType.SPN().String():
			tenId, appId, clientSecret := GlobalInputManager{}.GetServicePrincipalAuth()
			env = append(env,
				"AZCOPY_AUTO_LOGIN_TYPE="+common.Iff(oauthMode == "", common.EAutoLoginType.SPN().String(), oauthMode),
				"AZCOPY_SPA_APPLICATION_ID="+appId,
				"AZCOPY_SPA_CLIENT_SECRET="+clientSecret,
			)

			if tenId != "" {
				env = append(env, "AZCOPY_TENANT_ID="+tenId)
			}
		case "", common.EAutoLoginType.AzCLI().String():
			if os.Getenv("NEW_E2E_ENVIRONMENT") == AzurePipeline {
				// We are already logged in with AzCLI in Azure Pipeline
			} else {
				tenId, appId, clientSecret := GlobalInputManager{}.GetServicePrincipalAuth()
				args := []string{
					"login",
					"--service-principal",
					"-u=" + appId,
					"-p=" + clientSecret,
				}
				if tenId != "" {
					args = append(args, "--tenant="+tenId)
					env = append(env, "AZCOPY_TENANT_ID="+tenId)
				}

				out, err := exec.Command("az", args...).Output()
				if err != nil {
					e, ok := err.(*exec.ExitError)
					if ok {
						return CopyOrSyncCommandResult{}, false, fmt.Errorf("%s\n%s\nfailed to login with AzCli: %s", e.Stderr, out, err.Error())
					} else {
						return CopyOrSyncCommandResult{}, false, fmt.Errorf("failed to login with AzCli: %s", err.Error())
					}
				}
			}

			env = append(env, "AZCOPY_AUTO_LOGIN_TYPE=AzCLI")
		case "pscred":
			var script string
			if os.Getenv("NEW_E2E_ENVIRONMENT") == AzurePipeline {
				tenId, clientId, token := GlobalInputManager{}.GetWorkloadIdentity()
				cmd := `Connect-AzAccount -ApplicationId %s -Tenant %s -FederatedToken %s`
				script = fmt.Sprintf(cmd, clientId, tenId, token)
			} else {
				tenId, appId, clientSecret := GlobalInputManager{}.GetServicePrincipalAuth()
				cmd := `$secret = ConvertTo-SecureString -String %s -AsPlainText -Force;
				$cred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList %s, $secret;
				Connect-AzAccount -ServicePrincipal -Credential $cred`
				if tenId != "" {
					cmd += " -Tenant " + tenId
				}

				script = fmt.Sprintf(cmd, clientSecret, appId)
			}
			out, err := exec.Command("pwsh", "-Command", script).Output()
			if err != nil {
				e := err.(*exec.ExitError)
				e, ok := err.(*exec.ExitError)
				if ok {
					return CopyOrSyncCommandResult{}, false, fmt.Errorf("%s\n%s\nfailed to login with Powershell: %s", e.Stderr, out, err.Error())
				} else {
					return CopyOrSyncCommandResult{}, false, fmt.Errorf("failed to login with Powershell: %s", err.Error())
				}
			}
			env = append(env, "AZCOPY_AUTO_LOGIN_TYPE=PsCred")
		default:
			return CopyOrSyncCommandResult{}, false, errors.New("Unsupported OAuth mode " + oauthMode)
		}
	}

	if logDir != "" {
		env = append(env, "AZCOPY_LOG_LOCATION="+logDir)
		env = append(env, "AZCOPY_JOB_PLAN_LOCATION="+filepath.Join(logDir, "plans"))
	}

	out, err := t.execDebuggableWithOutput(GlobalInputManager{}.GetExecutablePath(), args, env, afterStart, chToStdin)

	wasClean := true
	stdErr := make([]byte, 0)
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			stdErr = capLen(ee.Stderr) // cap length of this, because it can be a panic. But don't cap stdout, because we need its last line in newCopyOrSyncCommandResult
			if len(stdErr) > 0 {
				wasClean = false // something was written to stderr, probably a panic
			}
		}
	}

	if wasClean {
		// either it succeeded, for it returned a failure code in a clean (non-panic) way.
		// In both cases, we want out to be parsed, to get us the job ID.  E.g. maybe 1 transfer out of several failed,
		// and that's what we'er actually testing for (so can't treat this as a fatal error).
		r, ok := newCopyOrSyncCommandResult(string(out))
		if ok {
			return r, true, err
		} else {
			err = fmt.Errorf("could not parse AzCopy output. Run error, if any, was '%w'", err)
		}
	}

	return CopyOrSyncCommandResult{},
		false,
		fmt.Errorf("azcopy run error: %w\n  with stderr: %s\n  and stdout: %s\n  from args %v", err, stdErr, out, args)
}

func (t *TestRunner) SetTransferStatusFlag(value string) {
	t.flags["with-status"] = value
}

func (t *TestRunner) ExecuteJobsShowCommand(jobID common.JobID, azcopyDir string) (JobsShowCommandResult, error) {
	args := append([]string{"jobs", "show", jobID.String()}, t.computeArgs()...)
	cmd := exec.Command(GlobalInputManager{}.GetExecutablePath(), args...)

	if azcopyDir != "" {
		cmd.Env = append(cmd.Env, "AZCOPY_JOB_PLAN_LOCATION="+filepath.Join(azcopyDir, "plans"))
	}

	out, err := cmd.Output()
	if err != nil {
		return JobsShowCommandResult{}, err
	}

	return newJobsShowCommandResult(string(out)), nil
}

type CopyOrSyncCommandResult struct {
	jobID       common.JobID
	finalStatus common.ListSyncJobSummaryResponse
}

func newCopyOrSyncCommandResult(rawOutput string) (CopyOrSyncCommandResult, bool) {
	lines := strings.Split(rawOutput, "\n")

	// parse out the final status
	// -2 because the last line is empty
	if len(lines) < 2 {
		return CopyOrSyncCommandResult{}, false
	}
	finalLine := lines[len(lines)-2]
	finalMsg := common.JsonOutputTemplate{}
	err := json.Unmarshal([]byte(finalLine), &finalMsg)
	if err != nil {
		return CopyOrSyncCommandResult{}, false
	}

	jobSummary := common.ListSyncJobSummaryResponse{} // this is a superset of ListJobSummaryResponse, so works for both copy and sync
	err = json.Unmarshal([]byte(finalMsg.MessageContent), &jobSummary)
	if err != nil {
		return CopyOrSyncCommandResult{}, false
	}

	return CopyOrSyncCommandResult{jobID: jobSummary.JobID, finalStatus: jobSummary}, true
}

func (c *CopyOrSyncCommandResult) GetTransferList(status common.TransferStatus, azcopyDir string) ([]common.TransferDetail, error) {
	runner := newTestRunner()
	runner.SetTransferStatusFlag(status.String())

	// invoke AzCopy to get the status from the plan files
	result, err := runner.ExecuteJobsShowCommand(c.jobID, azcopyDir)
	if err != nil {
		return make([]common.TransferDetail, 0), err
	}

	return result.transfers, nil
}

type JobsShowCommandResult struct {
	jobID     common.JobID
	transfers []common.TransferDetail
}

func newJobsShowCommandResult(rawOutput string) JobsShowCommandResult {
	lines := strings.Split(rawOutput, "\n")

	// parse out the final status
	// -3 because the last line is empty
	finalLine := lines[len(lines)-3]
	finalMsg := common.JsonOutputTemplate{}
	err := json.Unmarshal([]byte(finalLine), &finalMsg)
	if err != nil {
		panic(err)
	}

	listTransfersResponse := common.ListJobTransfersResponse{}
	err = json.Unmarshal([]byte(finalMsg.MessageContent), &listTransfersResponse)
	if err != nil {
		panic(err)
	}

	return JobsShowCommandResult{jobID: listTransfersResponse.JobID, transfers: listTransfersResponse.Details}
}
