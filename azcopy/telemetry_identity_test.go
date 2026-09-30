package azcopy

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInstallationIdentityProcessHelper(t *testing.T) {
	directory := os.Getenv("AZCOPY_TEST_IDENTITY_DIRECTORY")
	if directory == "" {
		t.Skip("child process only")
	}
	fmt.Println("IDENTITY=" + installationIDInDir(directory))
}

func TestInstallationIdentityIsStableAcrossProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	directory := t.TempDir()
	expected := installationIDInDir(directory)
	require.Len(t, expected, 32)
	executable, err := os.Executable()
	require.NoError(t, err)
	var children []*exec.Cmd
	for index := 0; index < 8; index++ {
		child := exec.CommandContext(ctx, executable, "-test.run=^TestInstallationIdentityProcessHelper$")
		child.Env = append(os.Environ(), "AZCOPY_TEST_IDENTITY_DIRECTORY="+directory)
		children = append(children, child)
	}
	results := make(chan string, len(children))
	for _, child := range children {
		go func(child *exec.Cmd) {
			output, err := child.CombinedOutput()
			if err != nil {
				results <- "ERROR: " + err.Error()
				return
			}
			results <- string(output)
		}(child)
	}
	for range children {
		output := <-results
		_, rest, found := strings.Cut(output, "IDENTITY=")
		require.True(t, found, output)
		identity, _, _ := strings.Cut(rest, "\n")
		identity = strings.TrimSpace(identity)
		require.Len(t, identity, 32)
		require.Equal(t, expected, identity)
		require.Equal(t, readInstallationID(filepath.Join(directory, installationIDFileName)), identity)
	}
}
