// +build linux

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
	"bufio"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/spf13/cobra"
)

const clfsToolName = "CLFSLoad.py"

const invalidContainerURLError = "the destination is not a valid Container URL with SAS, please refer to the examples"
const publicBlobEndpoint = "blob.core.windows.net"

var loadCmdRawInput = rawLoadCmdArgs{}

type rawLoadCmdArgs struct {
	// essential args and flags
	src        string
	dst        string
	newSession bool
	statePath  string

	// optional flags
	compression          string
	numConcurrentWorkers uint32
	maxErrorsToTolerate  uint32
	preserveHardlinks    bool
	logLevel             string
}

func (raw rawLoadCmdArgs) cook() (cookedLoadCmdArgs, error) {
	cooked := cookedLoadCmdArgs{
		src:        raw.src,
		newSession: raw.newSession,
		statePath:  raw.statePath,
	}

	if cooked.statePath == "" {
		return cooked, errors.New("please specify a state-path")
	}

	// check the source exists
	_, err := os.Stat(cooked.src)
	if err != nil {
		return cooked, errors.New("the source cannot be accessed due to error: " + err.Error())
	}

	cooked.src, err = filepath.Abs(cooked.src)
	if err != nil {
		return cooked, errors.New("the source cannot be accessed due to error: " + err.Error())
	}

	// check the destination is a valid container URL
	rawURL, err := url.Parse(raw.dst)
	if err != nil {
		return cooked, errors.New(invalidContainerURLError)
	}

	blobURLParts := azblob.NewBlobURLParts(*rawURL)
	if blobURLParts.BlobName != "" || !strings.Contains(blobURLParts.Host, publicBlobEndpoint) ||
		blobURLParts.ContainerName == "" || blobURLParts.SAS.Encode() == "" {
		return cooked, errors.New(invalidContainerURLError)
	}

	cooked.dstAccount = strings.TrimSuffix(blobURLParts.Host, "."+publicBlobEndpoint)
	cooked.dstContainer = blobURLParts.ContainerName
	cooked.dstSAS = blobURLParts.SAS.Encode()

	// pass along the optional flags
	cooked.optionalFlags = []string{
		fmt.Sprintf("--compression=%s", raw.compression),
		fmt.Sprintf("--preserve_hardlinks=%v", common.Iffint32(raw.preserveHardlinks, 1, 0)),
		fmt.Sprintf("--log_level=%s", raw.logLevel),
	}

	if raw.numConcurrentWorkers > 0 {
		cooked.optionalFlags = append(cooked.optionalFlags, fmt.Sprintf("--worker_thread_count=%v", raw.numConcurrentWorkers))
	}

	if raw.maxErrorsToTolerate > 0 {
		cooked.optionalFlags = append(cooked.optionalFlags, fmt.Sprintf("--retry_errors=%v", raw.maxErrorsToTolerate))
	}

	if raw.newSession {
		cooked.optionalFlags = append(cooked.optionalFlags, "--new")
	}

	return cooked, nil
}

type cookedLoadCmdArgs struct {
	src           string
	dstAccount    string
	dstContainer  string
	dstSAS        string
	newSession    bool
	statePath     string
	optionalFlags []string
}

// loadClfsCmd represents the load command
var loadClfsCmd = &cobra.Command{
	Use:   "clfs [local dir] [container URL]",
	Short: "Transfers local data into a Container and stores it in Microsoft's Avere Cloud FileSystem (CLFS) format",
	// Note: the help messages are kept in this file on purpose, to limit the footprint of the change (easier migration later)
	Long: `The load command copies data into Azure Blob Storage Containers and stores it in Microsoft's Avere Cloud FileSystem (CLFS) format. 
The proprietary CLFS format is used by the Azure HPC Cache and Avere vFXT for Azure products.

To leverage this command, please install the necessary extension via: pip3 install clfsload~=1.0.23. Please make sure CLFSLoad.py is 
in your PATH. For more information on this step, please visit https://aka.ms/azcopy/clfs.

This command is a simple option for moving existing data to cloud storage for use with specific Microsoft high-performance computing cache products. 
Because these products use a proprietary cloud filesystem format to manage data, that data cannot be loaded through the native copy command. 
Instead, the data must be loaded through the cache product itself OR via this load command, which uses the correct proprietary format.
This command lets you transfer data without using the cache - for example, 
to pre-populate storage or to add files to a working set without increasing cache load.

The destination is an empty Azure Storage Container. When the transfer is complete, the destination container can be used with an Azure HPC Cache instance or Avere vFXT for Azure cluster.

NOTE: This is a preview release of the load command. Please report any issues on the AzCopy Github repo.
`,
	Example: `
Load an entire directory with a SAS:
  - azcopy load clfs "/path/to/dir" "https://[account].blob.core.windows.net/[container]?[SAS]" --state-path="/path/to/state/path"
`,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			return errors.New("please pass two arguments: the path to the local source data, and the URL to the container")
		}
		loadCmdRawInput.src = args[0]
		loadCmdRawInput.dst = args[1]
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		clfsToolPath, err := exec.LookPath(clfsToolName)
		if err != nil {
			glcm.Error("cannot find CLFSLoad extension, please install it with: pip3 install clfsload")
		}

		cooked, err := loadCmdRawInput.cook()
		if err != nil {
			glcm.Error("Cannot start job due to error: " + err.Error())
			return
		}

		argsToInvokeExtension := []string{
			cooked.statePath,
			cooked.src,
			cooked.dstAccount,
			cooked.dstContainer,
			cooked.dstSAS,
			"--azcopy",
			"--dry_run",
			"--retry_errors=1",
		}
		argsToInvokeExtension = append(argsToInvokeExtension, cooked.optionalFlags...)

		glcm.Info("Invoking the CLFSLoad Extension located at: " + clfsToolPath)
		clfscmd := exec.Command(clfsToolPath, argsToInvokeExtension...)

		// hook up the stdout of the sub-process to our processor
		// stderr is piped directly
		out, err := clfscmd.StdoutPipe()
		if err != nil {
			panic(err)
		}
		clfsOutputParser := newClfsExtensionOutputParser(glcm)
		go clfsOutputParser.startParsing(bufio.NewReader(out))

		// let clfsload cancel itself, while we (the parent process) wait
		cancelChannel := make(chan os.Signal)
		go func() {
			// cancelChannel will be notified when os receives os.Interrupt and os.Kill signals
			signal.Notify(cancelChannel, os.Interrupt, os.Kill)

			for {
				select {
				case <-cancelChannel:
					glcm.Info("Cancellation requested. Beginning shutdown...")
					return
				}
			}
		}()

		err = clfscmd.Start()
		if err != nil {
			glcm.Error("Cannot start job due to error: " + err.Error())
		}

		clfsOutputParser.finishParsing()

		err = clfscmd.Wait()
		exitCode := common.EExitCode.Success()
		if err != nil {
			glcm.Error("Job failed due to extension error: " + err.Error())
			exitCode = common.EExitCode.Error()
		}

		glcm.Exit(func(format common.OutputFormat) string {
			return ""
		}, exitCode)
	},
}

func init() {
	loadCmd.AddCommand(loadClfsCmd)
	loadClfsCmd.PersistentFlags().BoolVar(&loadCmdRawInput.newSession, "new-session", true, "start a new job rather than continuing an existing one whose tracking information is kept at --state-path.")
	loadClfsCmd.PersistentFlags().StringVar(&loadCmdRawInput.statePath, "state-path", "", "required path to a local directory for job state tracking. The path must point to an existing directory in order to resume a job. It must be empty for a new job.")
	loadClfsCmd.PersistentFlags().StringVar(&loadCmdRawInput.compression, "compression-type", "LZ4", "specify the compression type to use for the transfers. Available values are: DISABLED,LZ4.")
	loadClfsCmd.PersistentFlags().Uint32Var(&loadCmdRawInput.maxErrorsToTolerate, "max-errors", 0, "specify the maximum number of transfer failures to tolerate. If enough errors occur, stop the job immediately.")
	loadClfsCmd.PersistentFlags().BoolVar(&loadCmdRawInput.preserveHardlinks, "preserve-hardlinks", false, "preserve hard link relationships.")
	loadClfsCmd.PersistentFlags().StringVar(&loadCmdRawInput.logLevel, "log-level", "INFO", "define the log verbosity for the log file, available levels: DEBUG, INFO, WARNING, ERROR.")
	loadClfsCmd.PersistentFlags().Uint32Var(&loadCmdRawInput.numConcurrentWorkers, "concurrency-count", 0, "override the number of parallel connections.")
	loadClfsCmd.PersistentFlags().MarkHidden("concurrency-count")

	// TODO remove after load command is implemented in Golang
	originalHelp := rootCmd.HelpFunc()
	rootCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		if cmd.Name() == loadCmd.Name() || (cmd.Parent() != nil && cmd.Parent().Name() == loadCmd.Name()) {
			cmd.Flags().MarkHidden("cap-mbps")
			cmd.Flags().MarkHidden("trusted-microsoft-suffixes")
		}
		originalHelp(cmd, args)
	})
}
