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

package cmd

import (
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var commandsWithJobAttemptTelemetry = map[string]struct{}{
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

// telemetryFlagAllowlist lists reviewed flags whose presence may be reported; unlisted flags are never reported.
// Values are recorded only for flags in telemetryFlagValuePolicies.
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
	"size-per-file": {}, "skip-version-check": {}, "tenant": {}, "trailing-dot": {},
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

// normalizeTelemetryBool records a flag value as "true" or "false"; anything else is dropped.
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
	if err != nil || parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return "", false
	}
	return strconv.FormatFloat(parsed, 'f', -1, 64), true
}

// normalizeTelemetryCategory lowercases an enum-like value; telemetryCategoryAllowed then keeps it only if it is a known value.
func normalizeTelemetryCategory(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || len(trimmed) > 128 {
		return "", false
	}
	return strings.ToLower(trimmed), true
}

var telemetryCategoryValues = map[string]string{
	"blob-type":                   "detect blockblob pageblob appendblob",
	"block-blob-tier":             "none hot cool cold archive",
	"check-md5":                   "nocheck logonly failifdifferent failifdifferentormissing",
	"compare-hash":                "none md5",
	"delete-destination":          "true false prompt",
	"delete-snapshots":            "none include only",
	"exclude-blob-type":           "blockblob pageblob appendblob",
	"from-to":                     "localblob localfile bloblocal filelocal blobpipe pipeblob filepipe filesmbpipe pipefile pipefilesmb blobtrash filetrash filesmbtrash blobfstrash localblobfs blobfslocal blobfsblobfs blobfsblob blobfsfile blobfsfilesmb blobblobfs fileblobfs filesmbblobfs blobblob fileblob filesmbblob blobfile blobfilesmb filefile s3blob gcpblob blobnone blobfsnone filenone localfilenfs filenfslocal filenfsfilenfs localfilesmb filesmblocal filesmbfilesmb filesmbfilenfs filenfsfilesmb benchmarkblob benchmarkfile benchmarkfilenfs benchmarkblobfs",
	"hardlinks":                   "follow preserve skip",
	"local-hash-storage-mode":     "hiddenfiles xattr alternatedatastreams",
	"login-type":                  "device spn msi azcli pscred workload",
	"mode":                        "upload download",
	"overwrite":                   "true false prompt ifsourcenewer posixproperties",
	"page-blob-tier":              "none p4 p6 p10 p15 p20 p30 p40 p50 p60 p70 p80",
	"permanent-delete":            "none snapshots versions snapshotsandversions",
	"posix-properties-style":      "standard amlfs",
	"rehydrate-priority":          "standard high",
	"s2s-handle-invalid-metadata": "excludeifinvalid renameifinvalid failifinvalid",
	"trailing-dot":                "enable disable allowtosafedestination",
}

func telemetryCategoryAllowed(flag, value string) bool {
	allowed, category := telemetryCategoryValues[flag]
	if !category {
		return true
	}
	if flag != "exclude-blob-type" && strings.Contains(value, ",") {
		return false
	}
	for _, item := range strings.Split(value, ",") {
		if item == "" || strings.ContainsAny(item, " \t\r\n") || !strings.Contains(" "+allowed+" ", " "+item+" ") {
			return false
		}
	}
	return true
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
	return ok || strings.HasPrefix(command, "completion.")
}

// telemetryOptions records only explicitly set flags and env vars; reviewed values become their own Opt* properties.
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
					if value, ok := policy.normalize(flag.Value.String()); ok && telemetryCategoryAllowed(flag.Name, value) {
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
