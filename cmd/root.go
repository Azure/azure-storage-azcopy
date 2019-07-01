// Copyright © 2017 Microsoft <wastore@microsoft.com>
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
	"bytes"
	"context"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/spf13/cobra"
	"net/url"
	"os"
	"runtime"
	"strings"
)

var azcopyAppPathFolder string
var azcopyLogPathFolder string
var azcopyMaxFileAndSocketHandles int
var outputFormatRaw string
var azcopyOutputFormat common.OutputFormat
var cmdLineCapMegaBitsPerSecond uint32

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Version: common.AzcopyVersion, // will enable the user to see the version info in the standard posix way: --version
	Use:     "azcopy",
	Short:   rootCmdShortDescription,
	Long:    rootCmdLongDescription,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {

		err := azcopyOutputFormat.Parse(outputFormatRaw)
		glcm.SetOutputFormat(azcopyOutputFormat)
		if err != nil {
			return err
		}

		// startup of the STE happens here, so that the startup can access the values of command line parameters that are defined for "root" command
		concurrentConnections := common.ComputeConcurrencyValue(runtime.NumCPU())
		concurrentFilesLimit := computeConcurrentFilesLimit(azcopyMaxFileAndSocketHandles, concurrentConnections)
		err = ste.MainSTE(concurrentConnections, concurrentFilesLimit, int64(cmdLineCapMegaBitsPerSecond), azcopyAppPathFolder, azcopyLogPathFolder)
		if err != nil {
			return err
		}

		// spawn a routine to fetch and compare the local application's version against the latest version available
		// if there's a newer version that can be used, then write the suggestion to stderr
		// however if this takes too long the message won't get printed
		// Note: this function is only triggered for non-help commands
		go detectNewVersion()

		return nil
	},
}

// hold a pointer to the global lifecycle controller so that commands could output messages and exit properly
var glcm = common.GetLifecycleMgr()

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(azsAppPathFolder, logPathFolder string, maxFileAndSocketHandles int) {
	azcopyAppPathFolder = azsAppPathFolder
	azcopyLogPathFolder = logPathFolder
	azcopyMaxFileAndSocketHandles = maxFileAndSocketHandles

	if err := rootCmd.Execute(); err != nil {
		glcm.Error(err.Error())
	} else {
		// our commands all control their own life explicitly with the lifecycle manager
		// only help commands reach this point
		// execute synchronously before exiting
		detectNewVersion()
		glcm.Exit(nil, common.EExitCode.Success())
	}
}

func init() {
	rootCmd.PersistentFlags().Uint32Var(&cmdLineCapMegaBitsPerSecond, "cap-mbps", 0, "caps the transfer rate, in Mega bits per second. Moment-by-moment throughput may vary slightly from the cap. If zero or omitted, throughput is not capped.")
	rootCmd.PersistentFlags().StringVar(&outputFormatRaw, "output-type", "text", "format of the command's output, the choices include: text, json.")

	// Special flag for generating test data
	// TODO: find a cleaner way to get the value into common, rather than just using it directly as a variable here
	rootCmd.PersistentFlags().StringVar(&common.SendRandomDataExt, "send-random-data-ext", "",
		"Files with this extension will not have their actual content sent. Instead, random data will be generated "+
			"and sent. The number of random bytes sent will equal the file size. To be used in testing. To use, use command-line "+
			"tools to create a sparse file of any desired size (but zero bytes actually used on-disk). Choose a distinctive"+
			"extension for the file (e.g. 'azCopySparseFill'). Then set this parameter to that extension (without the dot).")
	// On Windows, to create a sparse file, do something like this from an admin prompt:
	//     fsutil file createnew testfile.AzSparseFill 0
	//     fsutil sparse setflag .\testfile.AzSparseFill
	//     fsutil file seteof .\testfile.AzSparseFill 536870912000
	// Use dd on Linux.

	// Not making this publicly documented yet
	// TODO: add API calls to check that the on-disk size really is zero for the affected files, then make this publicly exposed
	rootCmd.PersistentFlags().MarkHidden("send-random-data-ext")
}

func detectNewVersion() {
	const versionMetadataUrl = "https://aka.ms/azcopyv10-version-metadata"

	// step 0: check the Stderr before checking version
	_, err := os.Stderr.Stat()
	if err != nil {
		return
	}

	// step 1: initialize pipeline
	p, err := createBlobPipeline(context.TODO(), common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()})
	if err != nil {
		return
	}

	// step 2: parse source url
	u, err := url.Parse(versionMetadataUrl)
	if err != nil {
		return
	}

	// step 3: start download
	blobURL := azblob.NewBlobURL(*u, p)
	blobStream, err := blobURL.Download(context.TODO(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return
	}

	blobBody := blobStream.Body(azblob.RetryReaderOptions{MaxRetryRequests: ste.MaxRetryPerDownloadBody})
	defer blobBody.Close()

	// step 4: read newest version str
	buf := new(bytes.Buffer)
	n, err := buf.ReadFrom(blobBody)
	if n == 0 || err != nil {
		return
	}
	// only take the first line, in case the version metadata file is upgraded in the future
	remoteVersion := strings.Split(buf.String(), "\n")[0]

	// step 5: compare remote version to local version to see if there's a newer AzCopy
	v1, err := NewVersion(common.AzcopyVersion)
	if err != nil {
		return
	}
	v2, err := NewVersion(remoteVersion)
	if err != nil {
		return
	}

	if v1.OlderThan(*v2) {
		executablePathSegments := strings.Split(strings.Replace(os.Args[0], "\\", "/", -1), "/")
		executableName := executablePathSegments[len(executablePathSegments)-1]

		// output in info mode instead of stderr, as it was crashing CI jobs of some people
		glcm.Info(executableName + ": A newer version " + remoteVersion + " is available to download\n")
	}
}

// ComputeConcurrentFilesLimit finds a number of concurrently-openable files
// such that we'll have enough handles left, after using some as network handles
// TODO: add environment var to optionally allow bringing concurrentFiles down lower
//    (and, when we do, actually USE it for uploads, since currently we're only using it on downloads)
//    (update logging
func computeConcurrentFilesLimit(maxFileAndSocketHandles int, concurrentConnections int) int {

	allowanceForOnGoingEnumeration := 1 // might still be scanning while we are transferring. Make this bigger if we ever do parallel scanning

	// Compute a very conservative estimate for total number of connections that we may have
	// To get a conservative estimate we pessimistically assume that the pool of idle conns is full,
	// but all the ones we are actually using are (by some fluke of timing) not in the pool.
	// TODO: consider actually SETTING AzCopyMaxIdleConnsPerHost to say, max(0.3 * FileAndSocketHandles, 1000), instead of using the hard-coded value we currently have
	possibleMaxTotalConcurrentHttpConnections := concurrentConnections + ste.AzCopyMaxIdleConnsPerHost + allowanceForOnGoingEnumeration

	concurrentFilesLimit := maxFileAndSocketHandles - possibleMaxTotalConcurrentHttpConnections

	if concurrentFilesLimit < ste.NumTransferInitiationRoutines {
		concurrentFilesLimit = ste.NumTransferInitiationRoutines // Set sensible floor, so we don't get negative or zero values if maxFileAndSocketHandles is low
	}
	return concurrentFilesLimit
}
