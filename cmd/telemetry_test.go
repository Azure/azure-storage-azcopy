package cmd

import (
	"sort"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

var telemetryFlagDenylist = map[string]struct{}{
	"aad-endpoint":               {},
	"application-id":             {},
	"await-continue":             {},
	"await-open":                 {},
	"certificate-path":           {},
	"debug-skip-files":           {},
	"destination-sas":            {},
	"hash-meta-dir":              {},
	"help":                       {},
	"identity-client-id":         {},
	"identity-object-id":         {},
	"identity-resource-id":       {},
	"list-of-files":              {},
	"list-of-versions":           {},
	"memory-profile":             {},
	"output-location":            {},
	"show-sensitive":             {},
	"source-sas":                 {},
	"tenant-id":                  {},
	"trusted-microsoft-suffixes": {},
}

func TestCommandTelemetryName(t *testing.T) {
	root := &cobra.Command{Use: "azcopy"}
	jobs := &cobra.Command{Use: "jobs"}
	jobsList := &cobra.Command{Use: "list", Aliases: []string{"ls"}}
	hiddenLoad := &cobra.Command{Use: "load", Hidden: true}
	deprecatedCLFS := &cobra.Command{Use: "clfs", Hidden: true, Deprecated: "deprecated"}

	root.AddCommand(jobs, hiddenLoad)
	jobs.AddCommand(jobsList)
	hiddenLoad.AddCommand(deprecatedCLFS)

	assert.Equal(t, "jobs.list", commandTelemetryName(jobsList))
	assert.Equal(t, "load.clfs", commandTelemetryName(deprecatedCLFS))
	assert.Equal(t, "", commandTelemetryName(root))
	assert.Equal(t, "", commandTelemetryName(nil))

	resolved, _, err := root.Find([]string{"jobs", "ls"})
	assert.NoError(t, err)
	assert.Equal(t, "jobs.list", commandTelemetryName(resolved))
}

func TestCommandUsesJobAttemptTelemetry(t *testing.T) {
	for _, command := range []string{"bench", "copy", "jobs.resume", "sync"} {
		assert.True(t, commandUsesJobAttemptTelemetry(command), command)
	}

	for _, command := range []string{
		"cancel",
		"doc",
		"env",
		"jobs.clean",
		"jobs.list",
		"jobs.remove",
		"jobs.show",
		"list",
		"load.clfs",
		"login",
		"login.status",
		"logout",
		"make",
		"pause",
		"remove",
		"set-properties",
	} {
		assert.False(t, commandUsesJobAttemptTelemetry(command), command)
	}
}

func TestCommandExcludedFromTelemetry(t *testing.T) {
	for _, command := range []string{
		"__complete",
		"__completeNoDesc",
		"completion",
		"doc",
		"env",
		"help",
		"load.clfs",
	} {
		assert.True(t, commandExcludedFromTelemetry(command), command)
	}

	for _, command := range []string{
		"jobs.clean",
		"jobs.list",
		"jobs.remove",
		"jobs.show",
		"list",
		"login",
		"login.status",
		"logout",
		"make",
		"remove",
		"set-properties",
	} {
		assert.False(t, commandExcludedFromTelemetry(command), command)
	}
}

func TestTelemetryOptions(t *testing.T) {
	for _, policy := range telemetryEnvironmentPolicies {
		t.Setenv(policy.environment.Name, "")
	}
	root := &cobra.Command{Use: "azcopy"}
	copyCmd := &cobra.Command{Use: "copy"}
	root.AddCommand(copyCmd)

	root.PersistentFlags().String("output-type", "text", "")
	copyCmd.Flags().Bool("recursive", false, "")
	copyCmd.Flags().Bool("preserve-info", true, "")
	copyCmd.Flags().Float64("block-size-mb", 0, "")
	copyCmd.Flags().String("include-path", "", "")
	copyCmd.Flags().String("tenant-id", "", "")
	copyCmd.Flags().String("list-of-files", "", "")
	copyCmd.Flags().String("future-unreviewed-flag", "", "")

	assert.NoError(t, root.PersistentFlags().Set("output-type", "json"))
	assert.NoError(t, copyCmd.Flags().Set("recursive", "true"))
	assert.NoError(t, copyCmd.Flags().Set("preserve-info", "false"))
	assert.NoError(t, copyCmd.Flags().Set("block-size-mb", "8.5"))
	assert.NoError(t, copyCmd.Flags().Set("include-path", "secret/customer/path"))
	assert.NoError(t, copyCmd.Flags().Set("tenant-id", "customer-tenant-id"))
	assert.NoError(t, copyCmd.Flags().Set("list-of-files", "C:\\secret\\files.txt"))
	assert.NoError(t, copyCmd.Flags().Set("future-unreviewed-flag", "future-secret"))

	options := telemetryOptions(copyCmd)
	assert.Equal(t, []string{"block-size-mb", "include-path", "output-type", "preserve-info", "recursive"}, options.FlagsSet)
	assert.Equal(t, map[string]string{
		"OptBlockSizeMB":  "8.5",
		"OptPreserveInfo": "false",
		"OptRecursive":    "true",
	}, options.Values)
	assert.Empty(t, options.EnvVarsSet)

	serialized := strings.Join(options.FlagsSet, ",")
	for key, value := range options.Values {
		serialized += key + value
	}
	for _, secret := range []string{"customer-tenant-id", "secret/customer/path", "C:\\secret\\files.txt", "future-secret"} {
		assert.NotContains(t, serialized, secret)
	}
}

func TestTelemetryOptionsExplicitEnvironmentOverrides(t *testing.T) {
	for _, policy := range telemetryEnvironmentPolicies {
		t.Setenv(policy.environment.Name, "")
	}
	t.Setenv(common.EEnvironmentVariable.ConcurrencyValue().Name, "AUTO")
	t.Setenv(common.EEnvironmentVariable.TransferInitiationPoolSize().Name, "48")
	t.Setenv(common.EEnvironmentVariable.EnumerationPoolSize().Name, "32")
	t.Setenv(common.EEnvironmentVariable.BufferGB().Name, "0.50")
	t.Setenv(common.EEnvironmentVariable.ParallelStatFiles().Name, "false")
	t.Setenv(common.EEnvironmentVariable.ShowPerfStates().Name, "1")

	options := telemetryOptions(&cobra.Command{Use: "copy"})
	assert.Equal(t, []string{
		"AZCOPY_BUFFER_GB",
		"AZCOPY_CONCURRENCY_VALUE",
		"AZCOPY_CONCURRENT_FILES",
		"AZCOPY_CONCURRENT_SCAN",
		"AZCOPY_PARALLEL_STAT_FILES",
		"AZCOPY_SHOW_PERF_STATES",
	}, options.EnvVarsSet)
	assert.Equal(t, map[string]string{
		"OptBufferGB":          "0.5",
		"OptConcurrency":       "auto",
		"OptConcurrentFiles":   "48",
		"OptConcurrentScan":    "32",
		"OptParallelStatFiles": "false",
	}, options.Values)
}

func TestTelemetryOptionsOmitDefaults(t *testing.T) {
	for _, policy := range telemetryEnvironmentPolicies {
		t.Setenv(policy.environment.Name, "")
	}
	command := &cobra.Command{Use: "copy"}
	command.Flags().Bool("recursive", true, "")

	options := telemetryOptions(command)
	assert.Empty(t, options.FlagsSet)
	assert.Empty(t, options.EnvVarsSet)
	assert.Empty(t, options.Values)
}

func TestEveryRegisteredFlagHasTelemetryClassification(t *testing.T) {
	var unclassified []string
	var overlap []string
	seen := make(map[string]struct{})

	var inspectFlags func(*cobra.Command)
	inspectFlags = func(command *cobra.Command) {
		visit := func(flags *pflag.FlagSet) {
			flags.VisitAll(func(flag *pflag.Flag) {
				if _, alreadySeen := seen[flag.Name]; alreadySeen {
					return
				}
				seen[flag.Name] = struct{}{}
				_, allowed := telemetryFlagAllowlist[flag.Name]
				_, denied := telemetryFlagDenylist[flag.Name]
				switch {
				case allowed && denied:
					overlap = append(overlap, flag.Name)
				case !allowed && !denied:
					unclassified = append(unclassified, flag.Name)
				}
			})
		}
		visit(command.LocalNonPersistentFlags())
		visit(command.PersistentFlags())
		for _, child := range command.Commands() {
			inspectFlags(child)
		}
	}

	inspectFlags(rootCmd)
	sort.Strings(unclassified)
	sort.Strings(overlap)
	assert.Empty(t, unclassified, "new flags must be explicitly allowed or denied")
	assert.Empty(t, overlap, "flags cannot be both allowed and denied")
}

func TestTelemetryValuePoliciesAreReviewed(t *testing.T) {
	for flag, policy := range telemetryFlagValuePolicies {
		_, allowed := telemetryFlagAllowlist[flag]
		_, denied := telemetryFlagDenylist[flag]
		assert.True(t, allowed, flag)
		assert.False(t, denied, flag)
		assert.True(t, strings.HasPrefix(policy.property, "Opt"), policy.property)
		assert.NotNil(t, policy.normalize, flag)
	}

	seenEnvironmentVariables := make(map[string]struct{})
	seenProperties := make(map[string]struct{})
	for _, policy := range telemetryEnvironmentPolicies {
		name := policy.environment.Name
		assert.NotEmpty(t, name)
		_, duplicateName := seenEnvironmentVariables[name]
		assert.False(t, duplicateName, name)
		seenEnvironmentVariables[name] = struct{}{}

		if policy.value.property == "" {
			assert.Nil(t, policy.value.normalize, name)
			continue
		}
		assert.True(t, strings.HasPrefix(policy.value.property, "Opt"), policy.value.property)
		assert.NotNil(t, policy.value.normalize, name)
		_, duplicateProperty := seenProperties[policy.value.property]
		assert.False(t, duplicateProperty, policy.value.property)
		seenProperties[policy.value.property] = struct{}{}
	}
}
