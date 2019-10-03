// +build linux darwin

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
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/spf13/cobra"
)

const clfsToolName = "CLFSLoad-1.0.11"
const clfsToolMD5Hash = "b1ef468e6b8953044e421423558fe396"

//const clfsToolName = "clfsload-mock"
//const clfsToolMD5Hash = "c9b050f29d271ad624a5732ca3795993"
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
	dryRun               bool
	dryRunGB             int
	dryRunCount          int
	compression          string
	numConcurrentWorkers int
	maxErrorsToTolerate  int
	preserveHardlinks    bool
}

func (raw rawLoadCmdArgs) cook() (cookedLoadCmdArgs, error) {
	cooked := cookedLoadCmdArgs{
		src:        raw.src,
		newSession: raw.newSession,
		statePath:  raw.statePath,
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

	return cooked, nil
}

type cookedLoadCmdArgs struct {
	src          string
	dstAccount   string
	dstContainer string
	dstSAS       string
	newSession   bool
	statePath    string
}

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:   "load [local dir] [container URL]",
	Short: "Transfers local data into a Container and stores it in Microsoft's Avere Cloud FileSystem (CLFS) format",
	// Note: the help messages are kept in this file on purpose, to limit the footprint of the change (easier migration later)
	Long: `The load command copies data into Azure Blob Storage Containers and stores it in Microsoft's Avere Cloud FileSystem (CLFS) format. 
The proprietary CLFS format is used by the Azure HPC Cache and Avere vFXT for Azure products.

This command is a simple option for moving existing data to cloud storage for use with specific Microsoft high-performance computing cache products. 
Because these products use a proprietary cloud filesystem format to manage data, you must populate storage by using the cache service 
instead of through a native copy command. This command lets you transfer data without using the cache - for example, 
to pre-populate storage or to add files to a working set without increasing cache load.

The destination is an empty Azure Storage Container. When the transfer is complete, the destination container can be used with an Azure HPC Cache instance or Avere vFXT for Azure cluster.

NOTE: This is a preview release of the load command. Please report any issues on Github. 
`,
	Example: `
Load an entire directory with a SAS:
  - azcopy cp "/path/to/dir" "https://[account].blob.core.windows.net/[container]?[SAS]" --state-path="/path/to/state/path"
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
		clfsToolPath, err := getClfsExtensionPathAndVerifyHash(clfsToolMD5Hash)
		if err != nil {
			glcm.Error(err.Error())
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
		}

		if cooked.newSession {
			argsToInvokeExtension = append(argsToInvokeExtension, "--new")
		}

		glcm.Info("Invoking the CLFSLoad Extension located at: " + clfsToolPath)
		clfscmd := exec.Command(clfsToolPath, argsToInvokeExtension...)

		// hook up the stdout of the sub-process to our processor
		// stderr is piped directly
		out, err := clfscmd.StderrPipe()
		if err != nil {
			panic(err)
		}
		//clfscmd.Stderr = os.Stderr
		clfsOutputParser := newClfsExtensionOutputParser(glcm)
		go clfsOutputParser.startParsing(bufio.NewReader(out))

		err = clfscmd.Start()
		if err != nil {
			glcm.Error("Cannot start job due to error: " + err.Error())
		}

		clfsOutputParser.finishParsing()

		err = clfscmd.Wait()
		if err != nil {
			glcm.Error("Job failed due to error: " + err.Error())
		}

		glcm.Exit(func(format common.OutputFormat) string {
			return ""
		}, common.EExitCode.Success())
	},
}

func init() {
	rootCmd.AddCommand(loadCmd)
	loadCmd.PersistentFlags().BoolVar(&loadCmdRawInput.newSession, "new-session", true, "TODO")
	loadCmd.PersistentFlags().StringVar(&loadCmdRawInput.statePath, "state-path", "", "TODO")
}

func getClfsExtensionPathAndVerifyHash(expectedHash string) (string, error) {
	currentParentDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		glcm.Error(err.Error())
	}

	clfsToolPath := filepath.Join(currentParentDir, clfsToolName)
	_, err = os.Stat(clfsToolPath)

	// in case the CLFS extension is not next to our azcopy executable, find it under the user's PATH
	if err != nil {
		clfsToolPath, err = exec.LookPath(clfsToolName)
		if err != nil {
			return "", errors.New("cannot find CLFSLoad extension, please put it next to AzCopy, or anywhere under your PATH")
		}
	}

	file, err := os.Open(clfsToolPath)
	if err != nil {
		return "", errors.New("cannot open CLFSLoad extension")
	}
	defer file.Close()

	//make sure we will be invoking the right CLFSLoad
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return clfsToolPath, errors.New("cannot compute hash of CLFSLoad extension")
	}

	calculatedMD5 := hex.EncodeToString(hash.Sum(nil)[:16])
	if calculatedMD5 != expectedHash {
		return clfsToolPath, errors.New("hash of CLFSLoad extension does not appear to be correct")
	}

	return clfsToolPath, nil
}
