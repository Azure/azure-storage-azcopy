// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package cmd

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/testSuite/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"

	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var outputFormatRaw string
var outputVerbosityRaw string
var logVerbosityRaw string
var cancelFromStdin bool
var outputFormat OutputFormat
var OutputLevel OutputVerbosity
var LogLevel common.LogLevel
var CapMbps float64
var SkipVersionCheck bool
var disableTelemetry bool
var telemetrySamplingRate float64

var TrustedSuffixes string
var azcopyAwaitContinue bool
var azcopyAwaitAllowOpenFiles bool
var isPipeDownload bool
var retryStatusCodes string
var debugMemoryProfile string
var checkAzCopyUpdates bool

var commandsWithJobAttemptTelemetry = map[string]struct{}{
	"bench":       {},
	"copy":        {},
	"jobs.resume": {},
	"sync":        {},
}

var commandsExcludedFromTelemetry = map[string]struct{}{
	"__complete":       {},
	"__completeNoDesc": {},
	"completion":       {},
	"doc":              {},
	"env":              {},
	"help":             {},
	"load.clfs":        {},
}

// telemetryFlagAllowlist contains reviewed flag names whose presence is safe
// to report. Values are never read. Unknown flags are excluded until reviewed.
var telemetryFlagAllowlist = map[string]struct{}{
	"as-subdir": {}, "backup": {}, "blob-tags": {}, "blob-type": {}, "block-blob-tier": {},
	"block-size-mb": {}, "cache-control": {}, "cancel-from-stdin": {}, "cap-mbps": {},
	"check-length": {}, "check-md5": {}, "check-version": {}, "compare-hash": {},
	"content-disposition": {}, "content-encoding": {}, "content-language": {}, "content-type": {},
	"cpk-by-name": {}, "cpk-by-value": {}, "decompress": {}, "delete-destination": {},
	"delete-destination-file": {}, "delete-snapshots": {}, "delete-test-data": {}, "disable-auto-decoding": {},
	"disable-telemetry": {}, "dry-run": {}, "endpoint": {}, "exclude": {},
	"exclude-attributes": {}, "exclude-blob-type": {}, "exclude-container": {}, "exclude-path": {},
	"exclude-pattern": {}, "exclude-regex": {}, "file-count": {}, "flush-threshold": {},
	"follow-symlinks": {}, "force-if-read-only": {}, "format": {}, "from-to": {}, "hardlinks": {},
	"identity": {}, "ignore-error-if-completed": {}, "include": {}, "include-after": {},
	"include-attributes": {}, "include-before": {},
	"include-directory-stub": {}, "include-path": {}, "include-pattern": {}, "include-regex": {},
	"include-root": {}, "local-hash-storage-mode": {}, "location": {}, "log-level": {},
	"login-type": {}, "machine-readable": {}, "mega-units": {}, "metadata": {},
	"method": {}, "mirror-mode": {}, "mode": {}, "no-guess-mime-type": {},
	"number-of-folders": {}, "output-level": {}, "output-type": {}, "overwrite": {},
	"page-blob-tier": {}, "permanent-delete": {}, "posix-properties-style": {},
	"preserve-info": {}, "preserve-last-modified-time": {}, "preserve-owner": {}, "preserve-permissions": {},
	"preserve-posix-properties": {}, "preserve-smb-info": {}, "preserve-smb-permissions": {},
	"preserve-symlinks": {}, "properties": {},
	"put-blob-size-mb": {}, "put-md5": {}, "quota-gb": {}, "recursive": {},
	"rehydrate-priority": {}, "request-priority": {}, "retry-status-codes": {}, "running-tally": {},
	"s2s-detect-source-changed": {}, "s2s-get-properties-in-backend": {},
	"s2s-handle-invalid-metadata": {}, "s2s-preserve-access-tier": {},
	"s2s-preserve-blob-tags": {}, "s2s-preserve-properties": {}, "service-principal": {},
	"size-per-file": {}, "skip-version-check": {}, "telemetry-sampling-rate": {}, "tenant": {}, "trailing-dot": {},
	"with-status": {},
}

type telemetryValuePolicy struct {
	property  string
	normalize func(string) (string, bool)
}

var telemetryFlagValuePolicies = map[string]telemetryValuePolicy{
	"as-subdir":                     {"OptAsSubdir", normalizeTelemetryBool},
	"backup":                        {"OptBackup", normalizeTelemetryBool},
	"blob-type":                     {"OptBlobType", normalizeTelemetryCategory},
	"block-blob-tier":               {"OptBlockBlobTier", normalizeTelemetryCategory},
	"block-size-mb":                 {"OptBlockSizeMB", normalizeTelemetryFloat},
	"cap-mbps":                      {"OptCapMbps", normalizeTelemetryFloat},
	"check-length":                  {"OptCheckLength", normalizeTelemetryBool},
	"check-md5":                     {"OptCheckMD5", normalizeTelemetryCategory},
	"compare-hash":                  {"OptCompareHash", normalizeTelemetryCategory},
	"decompress":                    {"OptDecompress", normalizeTelemetryBool},
	"delete-destination":            {"OptDeleteDestination", normalizeTelemetryCategory},
	"delete-destination-file":       {"OptDeleteDestinationFile", normalizeTelemetryBool},
	"delete-snapshots":              {"OptDeleteSnapshots", normalizeTelemetryCategory},
	"delete-test-data":              {"OptBenchmarkDeleteTestData", normalizeTelemetryBool},
	"disable-auto-decoding":         {"OptDisableAutoDecoding", normalizeTelemetryBool},
	"dry-run":                       {"OptDryRun", normalizeTelemetryBool},
	"exclude-blob-type":             {"OptExcludeBlobTypes", normalizeTelemetryCategoryList},
	"file-count":                    {"OptBenchmarkFileCount", normalizeTelemetryInt},
	"follow-symlinks":               {"OptFollowSymlinks", normalizeTelemetryBool},
	"force-if-read-only":            {"OptForceIfReadOnly", normalizeTelemetryBool},
	"from-to":                       {"OptFromTo", normalizeTelemetryCategory},
	"hardlinks":                     {"OptHardlinks", normalizeTelemetryCategory},
	"include-directory-stub":        {"OptIncludeDirectoryStub", normalizeTelemetryBool},
	"include-root":                  {"OptIncludeRoot", normalizeTelemetryBool},
	"local-hash-storage-mode":       {"OptLocalHashStorageMode", normalizeTelemetryCategory},
	"login-type":                    {"OptLoginType", normalizeTelemetryCategory},
	"mirror-mode":                   {"OptMirrorMode", normalizeTelemetryBool},
	"mode":                          {"OptBenchmarkMode", normalizeTelemetryCategory},
	"no-guess-mime-type":            {"OptNoGuessMimeType", normalizeTelemetryBool},
	"number-of-folders":             {"OptBenchmarkFolderCount", normalizeTelemetryInt},
	"overwrite":                     {"OptOverwrite", normalizeTelemetryCategory},
	"page-blob-tier":                {"OptPageBlobTier", normalizeTelemetryCategory},
	"permanent-delete":              {"OptPermanentDelete", normalizeTelemetryCategory},
	"posix-properties-style":        {"OptPosixPropertiesStyle", normalizeTelemetryCategory},
	"preserve-info":                 {"OptPreserveInfo", normalizeTelemetryBool},
	"preserve-last-modified-time":   {"OptPreserveLastModifiedTime", normalizeTelemetryBool},
	"preserve-owner":                {"OptPreserveOwner", normalizeTelemetryBool},
	"preserve-permissions":          {"OptPreservePermissions", normalizeTelemetryBool},
	"preserve-posix-properties":     {"OptPreservePosixProperties", normalizeTelemetryBool},
	"preserve-smb-info":             {"OptPreserveSMBInfo", normalizeTelemetryBool},
	"preserve-smb-permissions":      {"OptPreserveSMBPermissions", normalizeTelemetryBool},
	"preserve-symlinks":             {"OptPreserveSymlinks", normalizeTelemetryBool},
	"put-blob-size-mb":              {"OptPutBlobSizeMB", normalizeTelemetryFloat},
	"put-md5":                       {"OptPutMD5", normalizeTelemetryBool},
	"quota-gb":                      {"OptQuotaGB", normalizeTelemetryInt},
	"recursive":                     {"OptRecursive", normalizeTelemetryBool},
	"rehydrate-priority":            {"OptRehydratePriority", normalizeTelemetryCategory},
	"request-priority":              {"OptRequestPriority", normalizeTelemetryInt},
	"s2s-detect-source-changed":     {"OptS2SDetectSourceChanged", normalizeTelemetryBool},
	"s2s-get-properties-in-backend": {"OptS2SGetPropertiesInBackend", normalizeTelemetryBool},
	"s2s-handle-invalid-metadata":   {"OptS2SHandleInvalidMetadata", normalizeTelemetryCategory},
	"s2s-preserve-access-tier":      {"OptS2SPreserveAccessTier", normalizeTelemetryBool},
	"s2s-preserve-blob-tags":        {"OptS2SPreserveBlobTags", normalizeTelemetryBool},
	"s2s-preserve-properties":       {"OptS2SPreserveProperties", normalizeTelemetryBool},
	"service-principal":             {"OptServicePrincipal", normalizeTelemetryBool},
	"size-per-file":                 {"OptBenchmarkFileSizeBytes", normalizeTelemetrySize},
	"skip-version-check":            {"OptSkipVersionCheck", normalizeTelemetryBool},
	"trailing-dot":                  {"OptTrailingDot", normalizeTelemetryCategory},
}

type telemetryEnvironmentPolicy struct {
	environment common.EnvironmentVariable
	value       telemetryValuePolicy
}

var telemetryEnvironmentPolicies = []telemetryEnvironmentPolicy{
	{common.EEnvironmentVariable.ConcurrencyValue(), telemetryValuePolicy{"OptConcurrency", normalizeTelemetryConcurrency}},
	{common.EEnvironmentVariable.TransferInitiationPoolSize(), telemetryValuePolicy{"OptConcurrentFiles", normalizeTelemetryInt}},
	{common.EEnvironmentVariable.EnumerationPoolSize(), telemetryValuePolicy{"OptConcurrentScan", normalizeTelemetryInt}},
	{common.EEnvironmentVariable.BufferGB(), telemetryValuePolicy{"OptBufferGB", normalizeTelemetryFloat}},
	{common.EEnvironmentVariable.ParallelStatFiles(), telemetryValuePolicy{"OptParallelStatFiles", normalizeTelemetryBool}},
	{common.EEnvironmentVariable.AutoTuneToCpu(), telemetryValuePolicy{"OptTuneToCPU", normalizeTelemetryBool}},
	{common.EEnvironmentVariable.DisableHierarchicalScanning(), telemetryValuePolicy{"OptDisableHierarchicalScan", normalizeTelemetryBool}},
	{common.EEnvironmentVariable.PacePageBlobs(), telemetryValuePolicy{"OptPacePageBlobs", normalizeTelemetryBool}},
	{common.EEnvironmentVariable.RequestTryTimeout(), telemetryValuePolicy{"OptRequestTryTimeoutMinutes", normalizeTelemetryFloat}},
	{common.EEnvironmentVariable.DownloadToTempPath(), telemetryValuePolicy{"OptDownloadToTempPath", normalizeTelemetryBool}},
	{common.EEnvironmentVariable.OptimizeSparsePageBlobTransfers(), telemetryValuePolicy{"OptOptimizeSparsePageBlob", normalizeTelemetryBool}},
	{common.EEnvironmentVariable.CacheProxyLookup(), telemetryValuePolicy{}},
	{common.EEnvironmentVariable.DisableSyslog(), telemetryValuePolicy{}},
	{common.EEnvironmentVariable.ShowPerfStates(), telemetryValuePolicy{}},
}

func normalizeTelemetryBool(value string) (string, bool) {
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return "", false
	}
	return strconv.FormatBool(parsed), true
}

func normalizeTelemetryInt(value string) (string, bool) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 0 {
		return "", false
	}
	return strconv.FormatInt(parsed, 10), true
}

func normalizeTelemetryFloat(value string) (string, bool) {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil || parsed < 0 {
		return "", false
	}
	return strconv.FormatFloat(parsed, 'f', -1, 64), true
}

func normalizeTelemetryCategory(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || len(trimmed) > 128 {
		return "", false
	}
	return strings.ToLower(trimmed), true
}

func normalizeTelemetryCategoryList(value string) (string, bool) {
	parts := strings.FieldsFunc(value, func(r rune) bool { return r == ';' || r == ',' })
	if len(parts) == 0 || len(parts) > 16 {
		return "", false
	}
	for index := range parts {
		parts[index] = strings.ToLower(strings.TrimSpace(parts[index]))
		if parts[index] == "" || len(parts[index]) > 64 {
			return "", false
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ","), true
}

func normalizeTelemetrySize(value string) (string, bool) {
	bytes, err := ParseSizeString(strings.TrimSpace(value), common.SizePerFileParam)
	if err != nil || bytes < 0 {
		return "", false
	}
	return strconv.FormatInt(bytes, 10), true
}

func normalizeTelemetryConcurrency(value string) (string, bool) {
	if strings.EqualFold(strings.TrimSpace(value), "AUTO") {
		return "auto", true
	}
	return normalizeTelemetryInt(value)
}

func commandTelemetryName(cmd *cobra.Command) string {
	if cmd == nil {
		return ""
	}

	pathParts := strings.Fields(cmd.CommandPath())
	if len(pathParts) <= 1 {
		return ""
	}
	return strings.Join(pathParts[1:], ".")
}

func commandUsesJobAttemptTelemetry(command string) bool {
	_, ok := commandsWithJobAttemptTelemetry[command]
	return ok
}

func commandExcludedFromTelemetry(command string) bool {
	_, ok := commandsExcludedFromTelemetry[command]
	return ok
}

func telemetryOptions(cmd *cobra.Command) telemetry.OptionAttributes {
	if cmd == nil {
		return telemetry.OptionAttributes{}
	}

	seen := make(map[string]struct{})
	values := make(map[string]string)
	visit := func(flags *pflag.FlagSet) {
		flags.Visit(func(flag *pflag.Flag) {
			if _, allowed := telemetryFlagAllowlist[flag.Name]; allowed {
				seen[flag.Name] = struct{}{}
				if policy, capturesValue := telemetryFlagValuePolicies[flag.Name]; capturesValue {
					if value, ok := policy.normalize(flag.Value.String()); ok {
						values[policy.property] = value
					}
				}
			}
		})
	}
	visit(cmd.Flags())
	for current := cmd; current != nil; current = current.Parent() {
		visit(current.PersistentFlags())
	}

	flagsSet := make([]string, 0, len(seen))
	for name := range seen {
		flagsSet = append(flagsSet, name)
	}
	sort.Strings(flagsSet)

	var envVarsSet []string
	for _, policy := range telemetryEnvironmentPolicies {
		raw, explicitlySet := os.LookupEnv(policy.environment.Name)
		if !explicitlySet || strings.TrimSpace(raw) == "" {
			continue
		}
		envVarsSet = append(envVarsSet, policy.environment.Name)
		if policy.value.property != "" {
			if value, ok := policy.value.normalize(raw); ok {
				values[policy.value.property] = value
			}
		}
	}
	sort.Strings(envVarsSet)

	if len(values) == 0 {
		values = nil
	}
	return telemetry.OptionAttributes{
		FlagsSet:   flagsSet,
		EnvVarsSet: envVarsSet,
		Values:     values,
	}
}

// It would be preferable if this was a local variable, since it just gets altered and shot off to the STE
var debugSkipFiles string

var Client azcopy.Client

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Version: common.AzcopyVersion, // will enable the user to see the version info in the standard posix way: --version
	Use:     "azcopy",
	Short:   rootCmdShortDescription,
	Long:    rootCmdLongDescription,
	// PersistentPreRunE hook will not run on just `azcopy` without any subcommand
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		glcm.RegisterCloseFunc(func() {
			if debugMemoryProfile != "" {
				memProfDir := filepath.Dir(debugMemoryProfile)
				err := os.MkdirAll(memProfDir, 0777)
				if err != nil {
					panic(fmt.Sprintf("Failed to create memory profile parent dir: %v", err))
				}

				f, err := os.OpenFile(debugMemoryProfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
				if err != nil {
					panic(fmt.Sprintf("Failed to open memory profile: %v", err))
				}
				defer f.Close()
				runtime.GC()
				if err := pprof.WriteHeapProfile(f); err != nil {
					log.Fatal("could not write memory profile: ", err)
				}
			}
		})

		// referencing https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/azcore/policy/policy.go#L114
		rscList := "408;429;500;502;503;504"
		if retryStatusCodes != "" {
			rscList += ";" + retryStatusCodes
		}
		rsc, err := ste.ParseRetryCodes(rscList)
		if err != nil {
			return fmt.Errorf("failed to parse requested retry status code list: %w", err)
		}
		ste.RetryStatusCodes = rsc

		glcm.E2EEnableAwaitAllowOpenFiles(azcopyAwaitAllowOpenFiles)
		if azcopyAwaitContinue {
			glcm.E2EAwaitContinue()
		}

		err = outputFormat.Parse(outputFormatRaw)
		if err != nil {
			return err
		}

		err = OutputLevel.Parse(outputVerbosityRaw)
		if err != nil {
			return err
		}

		err = LogLevel.Parse(logVerbosityRaw)
		if err != nil {
			return err
		}

		// Check if we are downloading to Pipe so we can bypass version check and not write it to stdout, customer is
		// only expecting blob data in stdout
		var fromToFlagValue string
		if cmd.Flags().Changed("from-to") {
			// Access the value of the "from-to" flag
			fromToFlagValue, err = cmd.Flags().GetString("from-to")
			if err != nil {
				return fmt.Errorf("error accessing 'from-to' flag: %v", err)
			}
			if fromToFlagValue == "BlobPipe" {
				isPipeDownload = true
			}
		}

		// warn Windows users re quoting (since our docs all use single quotes, but CMD needs double)
		// Single ones just come through as part of the args, in CMD.
		// Ideally, for usability, we'd ideally have this info come back in the result of url.Parse. But that's hard to
		// arrange. So we check it here.
		if runtime.GOOS == "windows" {
			for _, a := range args {
				a = strings.ToLower(a)
				if strings.HasPrefix(a, "'http") { // note the single quote
					glcm.Info("")
					glcm.Info("*** When running from CMD, surround URLs with double quotes. Only using single quotes from PowerShell. ***")
					glcm.Info("")
					break
				}
			}
		}

		if debugSkipFiles != "" {
			for _, v := range strings.Split(debugSkipFiles, ";") {
				if strings.HasPrefix(v, "/") {
					v = strings.TrimPrefix(v, common.AZCOPY_PATH_SEPARATOR_STRING)
				}

				ste.DebugSkipFiles[v] = true
			}
		}

		// If the command is for resuming a job with a specific JobID,
		// use the provided JobID to resume the job; otherwise, create a new JobID.
		var resumeJobID common.JobID
		if cmd.Use == "resume [jobID]" {
			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command requires jobId to be passed as argument")
			}
			resumeJobID, err = common.ParseJobID(args[0])
			if err != nil {
				return err
			}
		}

		isBench := cmd.Use == "bench [destination]"

		// We only care to warn about multiple AzCopy processes for commands sent to STE
		sentToSte := []string{"copy [source] [destination]", "sync", "bench [destination]", "resume [jobID]", "remove [resourceURL]", "set-properties [source]"}
		var shouldWarn bool
		for _, currCmd := range sentToSte {
			if cmd.Use == currCmd {
				shouldWarn = true
				break
			}
		}
		isMigratedToLibrary := cmd.Use == "resume [jobID]" || cmd.Use == "sync" || cmd.Use == "copy [source] [destination]" || cmd.Use == "bench [destination]"
		if err := Initialize(isMigratedToLibrary, isBench, shouldWarn, resumeJobID); err != nil {
			return err
		}
		// Transfer job commands emit their own job-attempt start/finish events.
		// Other product-operation leaves emit one command.invoked event using
		// their canonical full command path; tooling-only commands are excluded.
		command := commandTelemetryName(cmd)
		if command != "" && !commandUsesJobAttemptTelemetry(command) && !commandExcludedFromTelemetry(command) {
			azcopy.ReportCommandInvoked(command, Client.CurrentJobID.String(), telemetryOptions(cmd))
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		// Version checking is done explicitly when the user sets flag
		if checkAzCopyUpdates && !isPipeDownload {
			select {
			// Either wait till this routine completes or timeout and do not print if it exceeds 8s
			// Spawn a routine to fetch & compare the local application's version against the latest version available
			case <-beginDetectNewVersion():
				// noop
			case <-time.After(time.Second * 8):
				// don't wait too long
			}
		}
		// Print out help command on just `azcopy`
		return cmd.Help()
	},
}

func Initialize(isMigratedToLibrary, isBench, shouldWarn bool, resumeJobId common.JobID) (err error) {
	glcm.SetOutputFormat(outputFormat)
	glcm.SetOutputVerbosity(OutputLevel)
	jobsAdmin.BenchmarkResults = isBench
	Client, err = azcopy.NewClient(azcopy.ClientOptions{
		CapMbps:               CapMbps,
		TrustedSuffixes:       TrustedSuffixes,
		LogLevel:              &LogLevel,
		DisableTelemetry:      disableTelemetry,
		TelemetrySamplingRate: &telemetrySamplingRate,
	})
	// Run MessagHandler to process messages from Input Watcher
	if jobsAdmin.JobsAdmin != nil {
		go jobsAdmin.JobsAdmin.MessageHandler(glcm.MsgHandlerChannel())
	}
	if err != nil {
		return err
	}

	Client.CurrentJobID = resumeJobId
	if Client.CurrentJobID.IsEmpty() {
		Client.CurrentJobID = common.NewJobID()
	}

	// We initialize the logger early because it needed for err-handling in the process checker
	common.AzcopyCurrentJobLogger = common.NewJobLogger(Client.CurrentJobID, LogLevel, common.LogPathFolder, "")
	common.AzcopyCurrentJobLogger.OpenLog()
	glcm.RegisterCloseFunc(func() {
		if common.AzcopyCurrentJobLogger != nil {
			common.AzcopyCurrentJobLogger.CloseLog()
		}
	})

	if !isMigratedToLibrary {
		timeAtPrestart := time.Now()

		// Log a clear ISO 8601-formatted start time, so it can be read and use in the --include-after parameter
		// Subtract a few seconds, to ensure that this date DEFINITELY falls before the LMT of any file changed while this
		// job is running. I.e. using this later with --include-after is _guaranteed_ to pick up all files that changed during
		// or after this job
		adjustedTime := timeAtPrestart.Add(-5 * time.Second)
		startTimeMessage := fmt.Sprintf("ISO 8601 START TIME: to copy files that changed before or after this job started, use the parameter --%s=%s or --%s=%s",
			common.IncludeBeforeFlagName, traverser.IncludeBeforeDateFilter{}.FormatAsUTC(adjustedTime),
			common.IncludeAfterFlagName, traverser.IncludeAfterDateFilter{}.FormatAsUTC(adjustedTime))
		common.LogToJobLogWithPrefix(startTimeMessage, common.LogInfo)
	}

	if shouldWarn {
		currPid := os.Getpid()
		AsyncWarnMultipleProcesses(cmd.GetAzCopyAppPath(), currPid) // Start the process checker
	}

	// For benchmarking, try to autotune if possible, otherwise use the default values
	if jobsAdmin.JobsAdmin != nil && isBench {
		envVar := common.EEnvironmentVariable.ConcurrencyValue()
		userValue := common.GetEnvironmentVariable(envVar)
		if userValue == "" || userValue == "auto" {
			jobsAdmin.JobsAdmin.SetConcurrencySettingsToAuto()
		} else {
			// Tell user that we can't actually auto tune, because configured value takes precedence
			// This case happens when benchmarking with a fixed value from the env var
			glcm.Info(fmt.Sprintf("Cannot auto-tune concurrency because it is fixed by environment variable %s", envVar.Name))
		}
	}
	if !isMigratedToLibrary {
		traverser.EnumerationParallelism, traverser.EnumerationParallelStatFiles = jobsAdmin.JobsAdmin.GetConcurrencySettings()
	}
	return nil

}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.

var Execute func() error = rootCmd.Execute

func InitializeAndExecute() {
	if err := Execute(); err != nil {
		glcm.Error(err.Error())
	} else {
		glcm.Exit(nil, EExitCode.Success())
	}
}

func init() {
	// replace the word "global" to avoid confusion (e.g. it doesn't affect all instances of AzCopy)
	rootCmd.SetUsageTemplate(strings.ReplaceAll((&cobra.Command{}).UsageTemplate(), "Global Flags", "Flags Applying to All Commands"))

	// the default value is set as -1 to differentiate from an input 3.
	// if unspecified, the policy doesn't set the request headers, which will cause the service to default to 3.
	// an explicit 3 will set the header and potentially upgrade x-ms-version.
	rootCmd.PersistentFlags().IntVar(&ste.GlobalRequestPriority, RequestPriorityFlag, -1, "Specify a request priority for Azure Storage to utilize in throttling from 0-7; priority is inverted, where 0 is the highest priority, and 7 is the lowest priority. The default is 3.")
	_ = rootCmd.PersistentFlags().MarkHidden(RequestPriorityFlag) // hide the request priority flag until official release

	rootCmd.PersistentFlags().Float64Var(&CapMbps, "cap-mbps", 0,
		"Caps the transfer rate, in megabits per second. "+
			"\n Moment-by-moment throughput might vary slightly from the cap."+
			"\n If this option is set to zero, or it is omitted, the throughput isn't capped.")
	rootCmd.PersistentFlags().StringVar(&outputFormatRaw, "output-type", "text",
		"Format of the command's output. The choices include: text, json. "+
			"\n The default value is 'text'.")
	rootCmd.PersistentFlags().StringVar(&outputVerbosityRaw, "output-level", "default",
		"Define the output verbosity. Available levels: essential, quiet.")
	rootCmd.PersistentFlags().StringVar(&logVerbosityRaw, "log-level", "INFO",
		"Define the log verbosity for the log file, "+
			"\n available levels: DEBUG(detailed trace), INFO(all requests/responses), WARNING(slow responses),"+
			"\n ERROR(only failed requests), and NONE(no output logs). (default 'INFO').")

	rootCmd.PersistentFlags().StringVar(&TrustedSuffixes, azcopy.TrustedSuffixesNameAAD, "",
		"\nSpecifies additional domain suffixes where Azure Active Directory login tokens may be sent.  \nThe default is '"+
			azcopy.TrustedSuffixesAAD+"'. \n Any listed here are added to the default. For security, you should only put Microsoft Azure domains here. "+
			"\n Separate multiple entries with semi-colons.")

	rootCmd.PersistentFlags().BoolVar(&SkipVersionCheck, "skip-version-check", false,
		"Do not perform the version check at startup. \nIntended for automation scenarios & airgapped use.")
	// Deprecated, marked as hidden to not break customers dependent on flag
	_ = rootCmd.PersistentFlags().MarkHidden("skip-version-check")
	// Note: this is due to Windows not supporting signals properly
	rootCmd.PersistentFlags().BoolVar(&cancelFromStdin, "cancel-from-stdin", false,
		"Used by partner teams to send in `cancel` through stdin to stop a job.")

	// special E2E testing flags
	rootCmd.PersistentFlags().BoolVar(&azcopyAwaitContinue, "await-continue", false,
		"Used when debugging, to tell AzCopy to await `continue` on stdin before starting any work. "+
			"\n Assists with debugging AzCopy via attach-to-process")
	rootCmd.PersistentFlags().BoolVar(&azcopyAwaitAllowOpenFiles, "await-open", false,
		"Used when debugging, to tell AzCopy to await `open` on stdin, after scanning but before opening the first file. "+
			"\n Assists with testing cases around file modifications between scanning and usage")
	rootCmd.PersistentFlags().StringVar(&debugSkipFiles, "debug-skip-files", "",
		"Used when debugging, to tell AzCopy to cancel the job midway."+
			"\n List of relative paths to skip in the STE.")

	// reserved for partner teams
	_ = rootCmd.PersistentFlags().MarkHidden("cancel-from-stdin")

	// special flags to be used in case of unexpected service errors.
	rootCmd.PersistentFlags().StringVar(&retryStatusCodes, "retry-status-codes", "",
		"Comma-separated list of HTTP status codes to retry on. (default '408;429;500;502;503;504')")
	_ = rootCmd.PersistentFlags().MarkHidden("retry-status-codes")
	rootCmd.PersistentFlags().StringVar(&debugMemoryProfile, "memory-profile", "", "Export pprof memory profile")
	_ = rootCmd.PersistentFlags().MarkHidden("memory-profile")
	rootCmd.PersistentFlags().BoolVar(&checkAzCopyUpdates, "check-version", false,
		"Check if a newer AzCopy version is available.")

	rootCmd.PersistentFlags().BoolVar(&disableTelemetry, "disable-telemetry", false,
		"Opt out of sending anonymous usage telemetry. Telemetry is enabled by default and contains no PII. "+
			"\n It can also be disabled by setting the AZCOPY_DISABLE_TELEMETRY environment variable to 'true'.")
	rootCmd.PersistentFlags().Float64Var(&telemetrySamplingRate, "telemetry-sampling-rate", 0.01,
		"Diagnostic override for the fraction of job IDs included in anonymous telemetry, from 0.0 through 1.0.")
	_ = rootCmd.PersistentFlags().MarkHidden("telemetry-sampling-rate")
}

// always spins up a new goroutine, because sometimes the aka.ms URL can't be reached (e.g. a constrained environment where
// aka.ms is not resolvable to a reachable IP address). In such cases, this routine will run for ever, and the caller should
// just give up on it.
// We spin up the GR here, not in the caller, so that the need to use a separate GC can never be forgotten
// (if do it synchronously, and can't resolve URL, this blocks caller for ever)
func beginDetectNewVersion() chan struct{} {
	completionChannel := make(chan struct{})
	go func() {
		// Step 0: check the Stderr, check local version
		_, err := os.Stderr.Stat()
		if err != nil {
			return
		}

		localVersion, err := NewVersion(common.AzcopyVersion)
		if err != nil {
			return
		}
		// Step 1: Fetch & validate cached version. If it is up to date, we return without making API calls
		filePath := filepath.Join(common.LogPathFolder, "latest_version.txt")
		cachedVersion, err := ValidateCachedVersion(filePath) // same as the remote version
		if err == nil {
			PrintOlderVersion(*cachedVersion, *localVersion)
		} else {
			// Step 2: Gets latest release on GitHub
			// If the cache version is expired, then we need to make a new API call
			// checking against latest Github release version
			gitHubRemoteVersion, err := getGitHubLatestRemoteVersion()
			if err != nil {
				return
			}
			PrintOlderVersion(*gitHubRemoteVersion, *localVersion)

			// Step 3: Persist  GitHub Remote version in local
			err = localVersion.CacheRemoteVersion(*gitHubRemoteVersion, filePath)
			if err != nil {
				return
			}
		}

		// let caller know we have finished, if they want to know
		close(completionChannel)
	}()

	return completionChannel
}

func getGitHubLatestRemoteVersionWithURL(apiEndpoint string) (*Version, error) {
	transport := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,  // GitHub API responses are small
		DisableKeepAlives:  false, // Connections are reused
	}

	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}
	// Get Request
	req, err := http.NewRequest("GET", apiEndpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("error in GitHub GET latest release: %s", resp.Status)
	}

	var release struct { // JSON response representation
		TagName string `json:"tag_name"`
		Name    string `json:"name"`
	}
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&release)
	if err != nil {
		return nil, err
	}
	// Remove v prefix in TagName, convert str to Version
	versionStr := strings.TrimPrefix(release.TagName, "v")
	return NewVersion(versionStr)
}

// Uses GitHub REST API to get the latest release version
func getGitHubLatestRemoteVersion() (*Version, error) {
	// GitHub REST API endpoint for latest release
	apiEndpoint := "https://api.github.com/repos/Azure/azure-storage-azcopy/releases/latest"
	return getGitHubLatestRemoteVersionWithURL(apiEndpoint)

}
