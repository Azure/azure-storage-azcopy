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
	"context"
	"errors"
	"fmt"
	pipeline2 "github.com/Azure/azure-pipeline-go/pipeline"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
)

type rawListCmdArgs struct {
	// obtained from argument
	sourcePath string

	Properties      string
	MachineReadable bool
	RunningTally    bool
	MegaUnits       bool
}

var validProperties = []string{
	"LastModifiedTime", "VersionId", "BlobType", "BlobAccessTier", "ContentType", "ContentEncoding",
}

func (raw *rawListCmdArgs) parseProperties(rawProperties string) []string {
	parsedProperties := make([]string, 0)
	listProperties := strings.Split(rawProperties, ";")
	for _, property := range listProperties {
		property = strings.TrimSpace(property)
		for _, validProperty := range validProperties {
			if len(property) != 0 && validProperty == property {
				parsedProperties = append(parsedProperties, property)
				break
			}
		}
	}
	return parsedProperties
}

func (raw rawListCmdArgs) cook() (cookedListCmdArgs, error) {
	cooked = cookedListCmdArgs{}
	// the expected argument in input is the container sas / or path of virtual directory in the container.
	// verifying the location type
	location := inferArgumentLocation(raw.sourcePath)
	// Only support listing for Azure locations
	if location != location.Blob() && location != location.File() && location != location.BlobFS() {
		return cooked, errors.New("invalid path passed for listing. given source is of type " + location.String() + " while expect is container / container path ")
	}
	cooked.sourcePath = raw.sourcePath
	cooked.MachineReadable = raw.MachineReadable
	cooked.RunningTally = raw.RunningTally
	cooked.MegaUnits = raw.MegaUnits
	cooked.location = location

	if raw.Properties != "" {
		cooked.properties = raw.parseProperties(raw.Properties)
	}

	return cooked, nil
}

type cookedListCmdArgs struct {
	sourcePath string
	location   common.Location

	properties      []string
	MachineReadable bool
	RunningTally    bool
	MegaUnits       bool
}

var raw rawListCmdArgs
var cooked cookedListCmdArgs

func init() {
	raw = rawListCmdArgs{}
	// listContainerCmd represents the list container command
	// listContainer list the blobs inside the container or virtual directory inside the container
	listContainerCmd := &cobra.Command{
		Use:     "list [containerURL]",
		Aliases: []string{"ls"},
		Short:   listCmdShortDescription,
		Long:    listCmdLongDescription,
		Example: listCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			// the listContainer command requires necessarily to have an argument

			// If no argument is passed then it is not valid
			// lsc expects the container path / virtual directory
			if len(args) == 0 || len(args) > 2 {
				return errors.New("this command only requires container destination")
			}
			raw.sourcePath = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			cooked, err := raw.cook()
			if err != nil {
				glcm.Error("failed to parse user input due to error: " + err.Error())
				return
			}
			err = cooked.HandleListContainerCommand()
			if err == nil {
				glcm.Exit(nil, common.EExitCode.Success())
			} else {
				glcm.Error(err.Error())
			}
		},
	}

	listContainerCmd.PersistentFlags().BoolVar(&raw.MachineReadable, "machine-readable", false, "Lists file sizes in bytes.")
	listContainerCmd.PersistentFlags().BoolVar(&raw.RunningTally, "running-tally", false, "Counts the total number of files and their sizes.")
	listContainerCmd.PersistentFlags().BoolVar(&raw.MegaUnits, "mega-units", false, "Displays units in orders of 1000, not 1024.")
	listContainerCmd.PersistentFlags().StringVar(&raw.Properties, "properties", "", "delimiter (;) separated values of properties required in list output.")

	rootCmd.AddCommand(listContainerCmd)
}

func (cooked cookedListCmdArgs) processProperties(object storedObject) string {
	builder := strings.Builder{}
	for _, property := range cooked.properties {
		switch property {
		case "LastModifiedTime":
			builder.WriteString(property + ": " + object.lastModifiedTime.String() + "; ")
		case "VersionId":
			builder.WriteString(property + ": " + object.blobVersionID + "; ")
		case "BlobType":
			builder.WriteString(property + ": " + string(object.blobType) + "; ")
		case "BlobAccessTier":
			builder.WriteString(property + ": " + string(object.blobAccessTier) + "; ")
		case "ContentType":
			builder.WriteString(property + ": " + object.contentType + "; ")
		case "ContentEncoding":
			builder.WriteString(property + ": " + object.contentEncoding + "; ")
		}
	}
	return builder.String()
}

// HandleListContainerCommand handles the list container command
func (cooked cookedListCmdArgs) HandleListContainerCommand() (err error) {
	// TODO: Temporarily use context.TODO(), this should be replaced with a root context from main.
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	credentialInfo := common.CredentialInfo{}

	source, err := SplitResourceString(cooked.sourcePath, cooked.location)
	if err != nil {
		return err
	}

	level, err := determineLocationLevel(source.Value, cooked.location, true)

	if err != nil {
		return err
	}

	// Treat our check as a destination because the isSource flag was designed for S2S transfers.
	if credentialInfo, _, err = getCredentialInfoForLocation(ctx, cooked.location, source.Value, source.SAS, false); err != nil {
		return fmt.Errorf("failed to obtain credential info: %s", err.Error())
	} else if cooked.location == cooked.location.File() && source.SAS == "" {
		return errors.New("azure files requires a SAS token for authentication")
	} else if credentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
		uotm := GetUserOAuthTokenManagerInstance()
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else {
			credentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	traverser, err := initResourceTraverser(source, cooked.location, &ctx, &credentialInfo, nil, nil, true, false,
		false, func(common.EntityType) {}, nil, false, pipeline2.LogNone)

	if err != nil {
		return fmt.Errorf("failed to initialize traverser: %s", err.Error())
	}

	var fileCount int64 = 0
	var sizeCount int64 = 0

	processor := func(object storedObject) error {
		path := object.relativePath
		if object.entityType == common.EEntityType.Folder() {
			path += "/" // TODO: reviewer: same questions as for jobs status: OK to hard code direction of slash? OK to use trailing slash to distinguish dirs from files?
		}

		properties := "; " + cooked.processProperties(object)
		objectSummary := path + properties + " Content Length: "

		if level == level.Service() {
			objectSummary = object.containerName + "/" + objectSummary
		}

		if cooked.MachineReadable {
			objectSummary += strconv.Itoa(int(object.size))
		} else {
			objectSummary += byteSizeToString(object.size)
		}

		if cooked.RunningTally {
			fileCount++
			sizeCount += object.size
		}

		glcm.Info(objectSummary)

		// No need to strip away from the name as the traverser has already done so.
		return nil
	}

	err = traverser.traverse(nil, processor, nil)

	if err != nil {
		return fmt.Errorf("failed to traverse container: %s", err.Error())
	}

	if cooked.RunningTally {
		glcm.Info("")
		glcm.Info("File count: " + strconv.Itoa(int(fileCount)))

		if cooked.MachineReadable {
			glcm.Info("Total file size: " + strconv.Itoa(int(sizeCount)))
		} else {
			glcm.Info("Total file size: " + byteSizeToString(sizeCount))
		}
	}

	return nil
}

//// printListContainerResponse prints the list container response
//func printListContainerResponse(lsResponse *common.ListContainerResponse) {
//	if len(lsResponse.Blobs) == 0 {
//		return
//	}
//	// TODO determine what's the best way to display the blobs in JSON
//	// TODO no partner team needs this functionality right now so the blobs are just outputted as info
//	for index := 0; index < len(lsResponse.Blobs); index++ {
//		glcm.Info(lsResponse.Blobs[index])
//	}
//}

var megaSize = []string{
	"B",
	"KB",
	"MB",
	"GB",
	"TB",
	"PB",
	"EB",
}

func byteSizeToString(size int64) string {
	units := []string{
		"B",
		"KiB",
		"MiB",
		"GiB",
		"TiB",
		"PiB",
		"EiB", // Let's face it, a file, account, or container probably won't be more than 1000 exabytes in YEARS. (and int64 literally isn't large enough to handle too many exbibytes. 128 bit processors when)
	}
	unit := 0
	floatSize := float64(size)
	gigSize := 1024

	if cooked.MegaUnits {
		gigSize = 1000
		units = megaSize
	}

	for floatSize/float64(gigSize) >= 1 {
		unit++
		floatSize /= float64(gigSize)
	}

	return strconv.FormatFloat(floatSize, 'f', 2, 64) + " " + units[unit]
}
