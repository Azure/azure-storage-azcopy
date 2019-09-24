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

const clfsToolName = "CLFSLoad-1.0.3"
const clfsToolMD5Hash = "6a644fddced3552a03416acbbf525844"
const invalidContainerURLError = "the destination is not a valid Container URL with SAS, please refer to the examples"
const publicBlobEndpoint = "blob.core.windows.net"

var loadCmdRawInput = rawLoadCmdArgs{}

type rawLoadCmdArgs struct {
	src        string
	dst        string
	newSession bool
	statePath  string
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
	Long: `The load command copies data into Azure Blob storage containers and stores it in Microsoft's Avere Cloud FileSystem (CLFS) format. 
The proprietary CLFS format is used by the Azure HPC Cache and Avere vFXT for Azure products.

This command is a simple option for moving existing data to cloud storage for use with specific Microsoft high-performance computing cache products. 
Because these products use a proprietary cloud filesystem format to manage data, you must populate storage by using the cache service 
instead of through a native copy command. This command lets you transfer data without using the cache - for example, 
to pre-populate storage or to add files to a working set without increasing cache load.

The destination is an Azure Storage container. When the transfer is complete, the destination container can be used with an Azure HPC Cache instance or Avere vFXT for Azure cluster.

NOTE: This is a preview release of the load command. Please report any issues on Github. 
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
		parentDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			glcm.Error(err.Error())
		}

		// make sure we will be invoking the right CLFSLoad
		clfsToolPath := filepath.Join(parentDir, clfsToolName)
		err = verifyExecutableHash(clfsToolPath, clfsToolMD5Hash)
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
		}

		if cooked.newSession {
			argsToInvokeExtension = append(argsToInvokeExtension, "--new")
		}

		clfscmd := exec.Command(clfsToolPath, argsToInvokeExtension...)
		clfscmd.Stdout = os.Stdout
		clfscmd.Stderr = os.Stderr
		err = clfscmd.Run()
		if err != nil {
			glcm.Error("Cannot finish job due to error: " + err.Error())
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

func verifyExecutableHash(path string, expectedHash string) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.New("cannot find CLFSLoad extension")
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return errors.New("cannot compute hash of CLFSLoad extension")
	}

	calculatedMD5 := hex.EncodeToString(hash.Sum(nil)[:16])
	if calculatedMD5 != expectedHash {
		return errors.New("hash of CLFSLoad extension does not appear to be correct")
	}

	return nil
}
