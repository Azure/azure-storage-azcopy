package e2etest

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
)

var _ AzCopyStdout = &AzCopyInteractiveStdout{}

// AzCopyInteractiveStdout is still a semi-raw stdout struct.
type AzCopyInteractiveStdout struct {
	Messages []string

	InputChan chan string    // Channel to signal user input
	wg        sync.WaitGroup // Wait group for synchronization
	mutex     sync.Mutex     // Mutex to synchronize access to waiting
	waiting   bool           // Flag to indicate if waiting is active

	asserter Asserter
}

// NewInteractiveWriter creates a new InteractiveWriter instance.
func NewAzCopyInteractiveStdout(a Asserter) *AzCopyInteractiveStdout {
	return &AzCopyInteractiveStdout{
		InputChan: make(chan string),
		asserter:  a,
	}
}

func (a *AzCopyInteractiveStdout) RawStdout() []string {
	return a.Messages
}

func (a *AzCopyInteractiveStdout) Write(p []byte) (n int, err error) {
	// Lock mutex to ensure exclusive access to waiting flag
	a.mutex.Lock()
	defer a.mutex.Unlock()

	str := string(p)
	lines := strings.Split(strings.TrimSuffix(str, "\n"), "\n")
	n = len(p)

	for _, v := range lines {
		a.Messages = append(a.Messages, v)
		a.asserter.Log(v)

	}
	if strings.Contains("To sign in", str) {
		// Set waiting flag
		a.waiting = true

		// Prompt user for input
		fmt.Fprint(os.Stderr, "Press Enter to continue: ")

		// Start waiting for user input in a separate goroutine
		a.wg.Add(1)
		go a.waitForInput()
	}

	return
}

// waitForInput waits for user input and sends it to the input channel.
func (a *AzCopyInteractiveStdout) waitForInput() {
	defer a.wg.Done()

	// Read user input from stdin
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')

	// Send user input to the input channel
	a.InputChan <- strings.TrimSpace(input)

	// Lock mutex to access waiting flag
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Reset waiting flag
	a.waiting = false
}

func (a *AzCopyInteractiveStdout) String() string {
	return strings.Join(a.RawStdout(), "\n")
}

func RunAzCopyLogin(a Asserter) AzCopyStdout {
	out := NewAzCopyInteractiveStdout(a)

	command := exec.Cmd{
		Path: GlobalConfig.AzCopyExecutableConfig.ExecutablePath,
		Args: []string{GlobalConfig.AzCopyExecutableConfig.ExecutablePath, "login"},

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
	a.Assert("wait for finalize", Not{IsNil{}}, err)
	a.Assert("expected exit code", Equal{}, 0, command.ProcessState.ExitCode())

	return out
}
