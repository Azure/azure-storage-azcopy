package common

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func setStatsMonitorTestEnv(t *testing.T) (logDir string) {
	t.Helper()
	oldLog, oldPlan, oldGlobal := LogPathFolder, AzcopyJobPlanFolder, GlobalSystemStatsMonitor
	logDir = t.TempDir()
	LogPathFolder, AzcopyJobPlanFolder, GlobalSystemStatsMonitor = logDir, t.TempDir(), nil
	t.Cleanup(func() {
		LogPathFolder, AzcopyJobPlanFolder, GlobalSystemStatsMonitor = oldLog, oldPlan, oldGlobal
	})
	return logDir
}

// openFDsTo counts this process's open file descriptors that point at path.
func openFDsTo(t *testing.T, path string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	count := 0
	for _, e := range entries {
		if target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); err == nil && target == path {
			count++
		}
	}
	return count
}

func TestStartSystemStatsMonitorSkipsNonLinux(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("only applies outside Linux")
	}
	setStatsMonitorTestEnv(t)

	stop, err := StartSystemStatsMonitor(NewJobID())
	require.NoError(t, err)
	require.NotNil(t, stop)
	require.Nil(t, GlobalSystemStatsMonitor)
	stop()
}

func TestStartSystemStatsMonitorIsIdempotentAndRestartable(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("stats monitor only runs on Linux")
	}
	setStatsMonitorTestEnv(t)

	stop, err := StartSystemStatsMonitor(NewJobID())
	require.NoError(t, err)
	first := GlobalSystemStatsMonitor
	require.NotNil(t, first)

	secondStop, err := StartSystemStatsMonitor(NewJobID())
	require.NoError(t, err)
	require.Same(t, first, GlobalSystemStatsMonitor, "a running monitor must not be replaced")
	secondStop() // must not stop the first monitor
	require.NoError(t, first.ctx.Err())

	stop()
	require.Error(t, first.ctx.Err(), "stop must cancel the monitor")
	stop() // safe to call twice

	restartStop, err := StartSystemStatsMonitor(NewJobID())
	require.NoError(t, err)
	require.NotSame(t, first, GlobalSystemStatsMonitor, "a stopped monitor must be replaced")
	restartStop()
}

func TestStopSystemStatsMonitorStopsRunningMonitor(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("stats monitor only runs on Linux")
	}
	setStatsMonitorTestEnv(t)

	StopSystemStatsMonitor() // nothing running: must be a no-op

	_, err := StartSystemStatsMonitor(NewJobID())
	require.NoError(t, err)
	running := GlobalSystemStatsMonitor

	StopSystemStatsMonitor()
	require.Error(t, running.ctx.Err(), "StopSystemStatsMonitor must stop the running monitor")

	restartStop, err := StartSystemStatsMonitor(NewJobID())
	require.NoError(t, err)
	require.NotSame(t, running, GlobalSystemStatsMonitor)
	restartStop()
}

func TestStartSystemStatsMonitorStopClosesLog(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("stats monitor only runs on Linux")
	}
	logDir := setStatsMonitorTestEnv(t)

	jobID := NewJobID()
	stop, err := StartSystemStatsMonitor(jobID)
	require.NoError(t, err)

	logPath := filepath.Join(logDir, jobID.String()+"-rolling-stats.log")
	require.FileExists(t, logPath)
	require.Positive(t, openFDsTo(t, logPath), "log should be open while the monitor runs")

	stop()
	require.Zero(t, openFDsTo(t, logPath), "stop must close the log")
}
