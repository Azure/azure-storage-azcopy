package claimedplan_test

import (
	"context"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

func runtimeConfig(t *testing.T) ste.JobExecutionSessionConfig {
	t.Helper()
	return ste.JobExecutionSessionConfig{
		JobID: common.NewJobID(), PlanDirectory: t.TempDir(),
		SourceRoot: "https://source.invalid/src", DestinationRoot: "https://dest.invalid/dst",
	}
}

func assertNoRuntimeArtifacts(t *testing.T, c ste.JobExecutionSessionConfig) {
	t.Helper()
	entries, err := os.ReadDir(c.PlanDirectory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("runtime created plan artifacts: %v, %v", entries, err)
	}
}

func TestClaimedRuntimeSettingsAndOwnership(t *testing.T) {
	t.Setenv("AZCOPY_CONCURRENCY_VALUE", "300")
	t.Setenv("AZCOPY_CONCURRENT_FILES", "64")
	t.Setenv("AZCOPY_CONCURRENT_SCAN", "16")
	t.Setenv("AZCOPY_CONCURRENT_SCHEDULERS", "1")
	c := runtimeConfig(t)
	settings, bytes, err := c.RuntimeSettings()
	if err != nil || bytes != 64<<20 || settings.InitialMainPoolSize != 300 || settings.MaxMainPoolSize.Value != 300 ||
		settings.TransferInitiationPoolSize.Value != 64 || settings.EnumerationPoolSize.Value != 16 ||
		settings.MaxIdleConnections != 300 || settings.MaxOpenDownloadFiles != 16 {
		t.Fatal("unexpected split-worker defaults", settings, bytes, err)
	}
	settings.InitialMainPoolSize, settings.MaxMainPoolSize.Value = 512, 512
	settings.TransferInitiationPoolSize.Value = 128
	settings.MaxIdleConnections = 512
	c.Concurrency, c.BufferBytes, c.CapMbps = &settings, 128<<20, 8
	resolved, bytes, err := c.RuntimeSettings()
	if err != nil || resolved.InitialMainPoolSize != 512 || resolved.TransferInitiationPoolSize.Value != 128 || bytes != 128<<20 {
		t.Fatal("explicit native resources were not resolved", resolved, bytes, err)
	}
	resolved.MaxMainPoolSize.Value = 99
	if settings.MaxMainPoolSize.Value != 512 {
		t.Fatal("resolved settings alias caller-owned pointers")
	}
	s, err := ste.NewJobExecutionSession(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.Status() != common.EJobStatus.Completed() || s.RetainedParts() != 0 {
		t.Fatal("capped session did not retire its resources")
	}
	assertNoRuntimeArtifacts(t, c)
}

func TestClaimedRuntimeConcurrencyFromEnvironment(t *testing.T) {
	t.Setenv("AZCOPY_CONCURRENT_FILES", "")
	t.Setenv("AZCOPY_CONCURRENT_SCAN", "")
	t.Setenv("AZCOPY_CONCURRENT_SCHEDULERS", "")
	main, files, scan, schedulers := 300, 64, 16, 1
	if buildmode.HighPerf() {
		main = max(main, buildmode.ConcurrencyValue())
		files, scan, schedulers = buildmode.ConcurrentFiles(), buildmode.ConcurrentScan(), buildmode.ConcurrentSchedulers()
	}
	for _, tc := range []struct {
		name, override string
		main           int
		wantError      string
	}{
		{name: "native-defaults", main: main},
		{name: "explicit-minimum", override: "300", main: 300},
		{name: "larger-override", override: "512", main: 512},
		{name: "below-minimum", override: "299", wantError: "at least 300"},
		{name: "old-test-override", override: "128", wantError: "at least 300"},
		{name: "unsupported-auto", override: "AUTO", wantError: "automatic tuning is not supported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AZCOPY_CONCURRENCY_VALUE", tc.override)
			s, _, err := (ste.JobExecutionSessionConfig{}).RuntimeSettings()
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("expected %q, got %v", tc.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if s.InitialMainPoolSize != tc.main || s.MaxMainPoolSize.Value != tc.main ||
				s.TransferInitiationPoolSize.Value != files || s.EnumerationPoolSize.Value != scan ||
				s.SchedulerParallelism.Value != schedulers || s.MaxIdleConnections != tc.main || s.MaxOpenDownloadFiles != 16 {
				t.Fatalf("unexpected native runtime settings: %+v", s)
			}
		})
	}
	t.Run("other-pool-overrides", func(t *testing.T) {
		t.Setenv("AZCOPY_CONCURRENCY_VALUE", "512")
		t.Setenv("AZCOPY_CONCURRENT_FILES", "128")
		t.Setenv("AZCOPY_CONCURRENT_SCAN", "32")
		t.Setenv("AZCOPY_CONCURRENT_SCHEDULERS", "8")
		s, _, err := (ste.JobExecutionSessionConfig{}).RuntimeSettings()
		if err != nil {
			t.Fatal(err)
		}
		if s.InitialMainPoolSize != 512 || s.TransferInitiationPoolSize.Value != 128 ||
			s.EnumerationPoolSize.Value != 32 || s.SchedulerParallelism.Value != 8 {
			t.Fatalf("native pool overrides were lost: %+v", s)
		}
	})
}

func TestSessionRuntimeRejectsInvalidWithoutArtifacts(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*ste.JobExecutionSessionConfig)
	}{
		{"buffer", func(c *ste.JobExecutionSessionConfig) { c.BufferBytes = 32 << 20 }},
		{"negative-cap", func(c *ste.JobExecutionSessionConfig) { c.CapMbps = -1 }},
		{"nan-cap", func(c *ste.JobExecutionSessionConfig) { c.CapMbps = math.NaN() }},
		{"infinite-cap", func(c *ste.JobExecutionSessionConfig) { c.CapMbps = math.Inf(1) }},
		{"tiny-cap", func(c *ste.JobExecutionSessionConfig) { c.CapMbps = 1e-10 }},
		{"negative-chunk-channel", func(c *ste.JobExecutionSessionConfig) { c.ChunkChannelSize = -1 }},
		{"oversized-chunk-channel", func(c *ste.JobExecutionSessionConfig) { c.ChunkChannelSize = 1000001 }},
		{"incomplete-pools", func(c *ste.JobExecutionSessionConfig) { c.Concurrency = &ste.ConcurrencySettings{} }},
		{"below-minimum", func(c *ste.JobExecutionSessionConfig) {
			settings, _, _ := c.RuntimeSettings()
			settings.InitialMainPoolSize, settings.MaxMainPoolSize.Value = 299, 299
			c.Concurrency = &settings
		}},
		{"autotune", func(c *ste.JobExecutionSessionConfig) {
			settings, _, _ := c.RuntimeSettings()
			settings.MaxMainPoolSize.Value++
			c.Concurrency = &settings
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := runtimeConfig(t)
			test.edit(&c)
			if s, err := ste.NewJobExecutionSession(context.Background(), c); err == nil {
				s.Close()
				t.Fatal("session accepted invalid runtime")
			}
			assertNoRuntimeArtifacts(t, c)
		})
	}
	c := runtimeConfig(t)
	c.SourceIsOAuth, c.SourceSAS = true, "sig=not-a-real-secret"
	if s, err := ste.NewJobExecutionSession(context.Background(), c); err == nil {
		s.Close()
		t.Fatal("ambiguous source authentication accepted")
	}
}

func TestSessionChunkChannelOverrideIsLocal(t *testing.T) {
	c := runtimeConfig(t)
	defaults := ste.GetChannelSizeConfig()
	settings, _, err := c.RuntimeSettings()
	if err != nil {
		t.Fatal(err)
	}
	settings.InitialMainPoolSize, settings.MaxMainPoolSize.Value = 512, 512
	settings.TransferInitiationPoolSize.Value, settings.SchedulerParallelism.Value = 64, 64
	settings.MaxIdleConnections = 512
	c.Concurrency, c.ChunkChannelSize = &settings, 300
	s, err := ste.NewJobExecutionSession(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p := s.Progress()
	if p.Channels.NormalChunkChannelSize != 300 || p.Channels.LowChunkChannelSize != 300 ||
		p.Channels.NormalTransferChannelSize != defaults.TransferChannelSize ||
		ste.GetChannelSizeConfig() != defaults {
		t.Fatalf("queue override leaked into other channels or global defaults: %+v", p.Channels)
	}
	c.ChunkChannelSize = 0
	other, err := ste.NewJobExecutionSession(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if got := other.Progress().Channels.NormalChunkChannelSize; got != defaults.ChunkChannelSize {
		t.Fatalf("default session inherited another binding's capacity: %d", got)
	}
}
