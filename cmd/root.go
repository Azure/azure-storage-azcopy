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
	"github.com/jiacfan/azure-storage-blob-go/azblob"
	"github.com/spf13/cobra"
	"net/url"
	"os"
	"strings"
	"time"
)

var azcopyAppPathFolder string
var azcopyLogPathFolder string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Version: common.AzcopyVersion, // will enable the user to see the version info in the standard posix way: --version
	Use:     "azcopy",
	Short:   rootCmdShortDescription,
	Long:    rootCmdLongDescription,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// spawn a routine to fetch and compare the local application's version against the latest version available
		// if there's a newer version that can be used, then write the suggestion to stderr
		// however if this takes too long the message won't get printed
		// Note: this function is only triggered for non-help commands
		go detectNewVersion()

	},
}

// hold a pointer to the global lifecycle controller so that commands could output messages and exit properly
var glcm = common.GetLifecycleMgr()

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(azsAppPathFolder, logPathFolder string) {
	azcopyAppPathFolder = azsAppPathFolder
	azcopyLogPathFolder = logPathFolder

	if err := rootCmd.Execute(); err != nil {
		glcm.Exit(err.Error(), common.EExitCode.Error())
	} else {
		// our commands all control their own life explicitly with the lifecycle manager
		// only help commands reach this point
		// execute synchronously before exiting
		detectNewVersion()
		glcm.Exit("", common.EExitCode.Success())
	}
}

func detectNewVersion() {
	const versionMetadataUrl = "https://aka.ms/azcopyv10-version-metadata"

	// step 0: check the Stderr before checking version
	_, err := os.Stderr.Stat()
	if err != nil {
		return
	}

	// step 1: initialize pipeline
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:        azblob.RetryPolicyExponential,
			MaxTries:      1,               // try a single time, if network is not available, just fail fast
			TryTimeout:    time.Second * 3, // don't wait for too long
			RetryDelay:    downloadRetryDelay,
			MaxRetryDelay: downloadMaxRetryDelay,
		},
		Telemetry: azblob.TelemetryOptions{
			Value: common.UserAgent,
		},
	})

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

	blobBody := blobStream.Body(azblob.RetryReaderOptions{MaxRetryRequests: downloadMaxTries})
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
		// print to stderr instead of stdout, in case the output is in other formats
		glcm.Error(executableName + ": A newer version " + remoteVersion + " is available to download\n")
	}
}
