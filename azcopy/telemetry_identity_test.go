package azcopy

import (
	"bufio"
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
	if os.Getenv("AZCOPY_TEST_IDENTITY_HOLD") == "1" {
		file, err := os.OpenFile(filepath.Join(directory, installationIDFileName+".lock"), os.O_RDWR|os.O_CREATE, 0600)
		require.NoError(t, err)
		defer file.Close()
		locked, err := tryInstallationLock(file)
		require.NoError(t, err)
		require.True(t, locked)
		fmt.Println("LOCKED")
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
		return
	}
	fmt.Println("IDENTITY=" + installationIDInDir(directory))
}

func TestInstallationIdentitySurvivesKilledWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	directory := t.TempDir()
	executable, err := os.Executable()
	require.NoError(t, err)
	writer := exec.CommandContext(ctx, executable, "-test.run=^TestInstallationIdentityProcessHelper$")
	writer.Env = append(os.Environ(), "AZCOPY_TEST_IDENTITY_DIRECTORY="+directory, "AZCOPY_TEST_IDENTITY_HOLD=1")
	stdin, err := writer.StdinPipe()
	require.NoError(t, err)
	defer stdin.Close()
	stdout, err := writer.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, writer.Start())
	t.Cleanup(func() { _ = writer.Process.Kill() })
	scanner := bufio.NewScanner(stdout)
	require.True(t, scanner.Scan())
	require.Equal(t, "LOCKED", scanner.Text())
	require.NoError(t, os.WriteFile(filepath.Join(directory, installationIDFileName), []byte("partial"), 0600))
	require.NoError(t, writer.Process.Kill())
	_ = writer.Wait()
	var children []*exec.Cmd
	for index := 0; index < 8; index++ {
		child := exec.CommandContext(ctx, executable, "-test.run=^TestInstallationIdentityProcessHelper$")
		child.Env = append(os.Environ(), "AZCOPY_TEST_IDENTITY_DIRECTORY="+directory, "AZCOPY_TEST_IDENTITY_HOLD=0")
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
		require.Equal(t, readInstallationID(filepath.Join(directory, installationIDFileName)), identity)
	}
}
