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
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

type rawListCmdArgs struct {
	// obtained from argument
	sourcePath string

	Properties      string
	MachineReadable bool
	RunningTally    bool
	MegaUnits       bool
	trailingDot     string
}

type validProperty string

const (
	lastModifiedTime validProperty = "LastModifiedTime"
	versionId        validProperty = "VersionId"
	blobType         validProperty = "BlobType"
	blobAccessTier   validProperty = "BlobAccessTier"
	contentType      validProperty = "ContentType"
	contentEncoding  validProperty = "ContentEncoding"
	contentMD5       validProperty = "ContentMD5"
	leaseState       validProperty = "LeaseState"
	leaseDuration    validProperty = "LeaseDuration"
	leaseStatus      validProperty = "LeaseStatus"
	archiveStatus    validProperty = "ArchiveStatus"

	versionIdTimeFormat = "2006-01-02T15:04:05.9999999Z"
)

// containsProperty checks if the property array contains a valid property
func containsProperty(properties []validProperty, prop validProperty) bool {
	for _, item := range properties {
		if item == prop {
			return true
		}
	}
	return false
}

// validProperties returns an array of possible values for the validProperty const type.
func validProperties() []validProperty {
	return []validProperty{lastModifiedTime, versionId, blobType, blobAccessTier,
		contentType, contentEncoding, contentMD5, leaseState, leaseDuration, leaseStatus, archiveStatus}
}

func (raw *rawListCmdArgs) parseProperties(rawProperties string) []validProperty {
	parsedProperties := make([]validProperty, 0)
	listProperties := strings.Split(rawProperties, ";")
	for _, p := range listProperties {
		for _, vp := range validProperties() {
			// check for empty string and also ignore the case
			if len(p) != 0 && strings.EqualFold(string(vp), p) {
				parsedProperties = append(parsedProperties, vp)
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
	location := InferArgumentLocation(raw.sourcePath)
	// Only support listing for Azure locations
	if location != location.Blob() && location != location.File() && location != location.BlobFS() {
		return cooked, errors.New("invalid path passed for listing. given source is of type " + location.String() + " while expect is container / container path ")
	}
	cooked.sourcePath = raw.sourcePath
	cooked.MachineReadable = raw.MachineReadable
	cooked.RunningTally = raw.RunningTally
	cooked.MegaUnits = raw.MegaUnits
	cooked.location = location
	err := cooked.trailingDot.Parse(raw.trailingDot)
	if err != nil {
		return cooked, err
	}

	if raw.Properties != "" {
		cooked.properties = raw.parseProperties(raw.Properties)
	}

	return cooked, nil
}

type cookedListCmdArgs struct {
	sourcePath string
	location   common.Location

	properties      []validProperty
	MachineReadable bool
	RunningTally    bool
	MegaUnits       bool
	trailingDot     common.TrailingDotOption
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
	listContainerCmd.PersistentFlags().StringVar(&raw.trailingDot, "trailing-dot", "", "'Enable' by default to treat file share related operations in a safe manner. Available options: Enable, Disable. "+
		"Choose 'Disable' to go back to legacy (potentially unsafe) treatment of trailing dot files where the file service will trim any trailing dots in paths. This can result in potential data corruption if the transfer contains two paths that differ only by a trailing dot (ex: mypath and mypath.). If this flag is set to 'Disable' and AzCopy encounters a trailing dot file, it will warn customers in the scanning log but will not attempt to abort the operation."+
		"If the destination does not support trailing dot files (Windows or Blob Storage), AzCopy will fail if the trailing dot file is the root of the transfer and skip any trailing dot paths encountered during enumeration.")

	rootCmd.AddCommand(listContainerCmd)
}

func (cooked cookedListCmdArgs) processProperties(object StoredObject) string {
	builder := strings.Builder{}
	for _, property := range cooked.properties {
		propertyStr := string(property)
		switch property {
		case lastModifiedTime:
			builder.WriteString(propertyStr + ": " + object.lastModifiedTime.String() + "; ")
		case versionId:
			builder.WriteString(propertyStr + ": " + object.blobVersionID + "; ")
		case blobType:
			builder.WriteString(propertyStr + ": " + string(object.blobType) + "; ")
		case blobAccessTier:
			builder.WriteString(propertyStr + ": " + string(object.blobAccessTier) + "; ")
		case contentType:
			builder.WriteString(propertyStr + ": " + object.contentType + "; ")
		case contentEncoding:
			builder.WriteString(propertyStr + ": " + object.contentEncoding + "; ")
		case contentMD5:
			builder.WriteString(propertyStr + ": " + base64.StdEncoding.EncodeToString(object.md5) + "; ")
		case leaseState:
			builder.WriteString(propertyStr + ": " + string(object.leaseState) + "; ")
		case leaseStatus:
			builder.WriteString(propertyStr + ": " + string(object.leaseStatus) + "; ")
		case leaseDuration:
			builder.WriteString(propertyStr + ": " + string(object.leaseDuration) + "; ")
		case archiveStatus:
			builder.WriteString(propertyStr + ": " + string(object.archiveStatus) + "; ")
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

	if err := common.VerifyIsURLResolvable(raw.sourcePath); cooked.location.IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve target: %w", err)
	}

	level, err := DetermineLocationLevel(source.Value, cooked.location, true)

	if err != nil {
		return err
	}

	// isSource is rather misnomer for canBePublic. We can list public containers, and hence isSource=true
	if credentialInfo, _, err = GetCredentialInfoForLocation(ctx, cooked.location, source.Value, source.SAS, true, common.CpkOptions{}); err != nil {
		return fmt.Errorf("failed to obtain credential info: %s", err.Error())
	} else if cooked.location == cooked.location.File() && source.SAS == "" {
		return errors.New("azure files requires a SAS token for authentication")
	} else if credentialInfo.CredentialType.IsAzureOAuth() {
		uotm := GetUserOAuthTokenManagerInstance()
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else {
			credentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	// check if user wants to get version id
	shouldGetVersionId := containsProperty(cooked.properties, versionId)

	traverser, err := InitResourceTraverser(source, cooked.location, &ctx, &credentialInfo, common.ESymlinkHandlingType.Skip(), nil, true, true, false, common.EPermanentDeleteOption.None(), func(common.EntityType) {}, nil, false, common.ESyncHashType.None(), common.EPreservePermissionsOption.None(), common.LogNone, common.CpkOptions{}, nil, false, cooked.trailingDot, nil, nil, shouldGetVersionId)

	if err != nil {
		return fmt.Errorf("failed to initialize traverser: %s", err.Error())
	}

	var fileCount int64 = 0
	var sizeCount int64 = 0

	type versionIdObject struct {
		versionId string
		fileSize  int64
	}
	objectVer := make(map[string]versionIdObject)

	processor := func(object StoredObject) error {
		path := object.relativePath
		if object.entityType == common.EEntityType.Folder() {
			path += "/" // TODO: reviewer: same questions as for jobs status: OK to hard code direction of slash? OK to use trailing slash to distinguish dirs from files?
		}

		properties := "; " + cooked.processProperties(object)
		objectSummary := path + properties + " Content Length: "

		if level == level.Service() {
			objectSummary = object.ContainerName + "/" + objectSummary
		}

		if cooked.MachineReadable {
			objectSummary += strconv.Itoa(int(object.size))
		} else {
			objectSummary += byteSizeToString(object.size)
		}

		if cooked.RunningTally {
			if shouldGetVersionId {
				// get new version id object
				updatedVersionId := versionIdObject{
					versionId: object.blobVersionID,
					fileSize:  object.size,
				}

				// there exists a current version id of the object
				if currentVersionId, ok := objectVer[object.relativePath]; ok {
					// get current version id time
					currentVid, _ := time.Parse(versionIdTimeFormat, currentVersionId.versionId)

					// get new version id time
					newVid, _ := time.Parse(versionIdTimeFormat, object.blobVersionID)

					// if new vid came after the current vid, then it is the latest version
					// update the objectVer with the latest version
					// we will also remove sizeCount and fileCount of current object, allowing
					// the updated sizeCount and fileCount to be added at line 320
					if newVid.After(currentVid) {
						sizeCount -= currentVersionId.fileSize // remove size of current object
						fileCount--                            // remove current object file count
						objectVer[object.relativePath] = updatedVersionId
					}
				} else {
					objectVer[object.relativePath] = updatedVersionId
				}
			}
			fileCount++
			sizeCount += object.size
		}

		glcm.Info(objectSummary)

		// No need to strip away from the name as the traverser has already done so.
		return nil
	}

	err = traverser.Traverse(nil, processor, nil)

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
		"EiB", // Let's face it, a file, account, or container probably won't be more than 1000 exabytes in YEARS.
		// (and int64 literally isn't large enough to handle too many exbibytes. 128 bit processors when)
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
