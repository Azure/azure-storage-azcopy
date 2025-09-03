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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	traverser2 "github.com/Azure/azure-storage-azcopy/v10/traverser"

	"github.com/spf13/cobra"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

type rawListCmdArgs struct {
	// obtained from argument
	src      string
	location string

	Properties      string
	MachineReadable bool
	RunningTally    bool
	MegaUnits       bool
	trailingDot     string
}

type validProperty string

const (
	LastModifiedTime validProperty = "LastModifiedTime"
	VersionId        validProperty = "VersionId"
	BlobType         validProperty = "BlobType"
	BlobAccessTier   validProperty = "BlobAccessTier"
	ContentType      validProperty = "ContentType"
	ContentEncoding  validProperty = "ContentEncoding"
	ContentMD5       validProperty = "ContentMD5"
	LeaseState       validProperty = "LeaseState"
	LeaseDuration    validProperty = "LeaseDuration"
	LeaseStatus      validProperty = "LeaseStatus"
	ArchiveStatus    validProperty = "ArchiveStatus"

	versionIdTimeFormat    = "2006-01-02T15:04:05.9999999Z"
	LastModifiedTimeFormat = "2006-01-02 15:04:05 +0000 GMT"
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
	return []validProperty{LastModifiedTime, VersionId, BlobType, BlobAccessTier,
		ContentType, ContentEncoding, ContentMD5, LeaseState, LeaseDuration, LeaseStatus, ArchiveStatus}
}

// validPropertiesString returns an array of valid properties in string array.
func validPropertiesString() []string {
	var propertiesArray []string
	for _, prop := range validProperties() {
		propertiesArray = append(propertiesArray, string(prop))
	}
	return propertiesArray
}

func (raw rawListCmdArgs) parseProperties() []validProperty {
	parsedProperties := make([]validProperty, 0)
	if raw.Properties != "" {
		listProperties := strings.Split(raw.Properties, ";")
		for _, p := range listProperties {
			for _, vp := range validProperties() {
				// check for empty string and also ignore the case
				if len(p) != 0 && strings.EqualFold(string(vp), p) {
					parsedProperties = append(parsedProperties, vp)
					break
				}
			}
		}
	}
	return parsedProperties
}

func (raw rawListCmdArgs) cook() (cookedListCmdArgs, error) {
	// set up the front end scanning logger
	common.AzcopyScanningLogger = common.NewJobLogger(Client.CurrentJobID, Client.GetLogLevel(), common.LogPathFolder, "-scanning")
	common.AzcopyScanningLogger.OpenLog()
	glcm.RegisterCloseFunc(func() {
		common.AzcopyScanningLogger.CloseLog()
	})
	cooked = cookedListCmdArgs{}
	// the expected argument in input is the container sas / or path of virtual directory in the container.
	// verifying the location type
	var err error
	cooked.location, err = ValidateArgumentLocation(raw.src, raw.location)
	if err != nil {
		return cooked, err
	}
	// Only support listing for Azure locations
	switch cooked.location {
	case common.ELocation.Blob():
	case common.ELocation.File(), common.ELocation.FileNFS():
	case common.ELocation.BlobFS():
		break
	default:
		return cooked, fmt.Errorf("azcopy only supports Azure resources for listing i.e. Blob, File, BlobFS")
	}
	cooked.sourcePath = raw.src
	cooked.MachineReadable = raw.MachineReadable
	cooked.RunningTally = raw.RunningTally
	cooked.MegaUnits = raw.MegaUnits
	err = cooked.trailingDot.Parse(raw.trailingDot)
	if err != nil {
		return cooked, err
	}
	cooked.properties = raw.parseProperties()

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
			if len(args) != 1 {
				return errors.New("this command only requires container destination")
			}
			raw.src = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			cooked, err := raw.cook()
			if err != nil {
				glcm.Error("failed to parse user input due to error: " + err.Error())
				return
			}
			err = cooked.handleListContainerCommand()
			if err == nil {
				glcm.Exit(nil, common.EExitCode.Success())
			} else {
				glcm.Error(err.Error() + getErrorCodeUrl(err))
			}
		},
	}

	listContainerCmd.PersistentFlags().StringVar(&raw.location, "location", "", "Optionally specifies the location. For Example: Blob, File, BlobFS")
	listContainerCmd.PersistentFlags().BoolVar(&raw.MachineReadable, "machine-readable", false, "False by default. Lists file sizes in bytes.")
	listContainerCmd.PersistentFlags().BoolVar(&raw.RunningTally, "running-tally", false, "False by default. Counts the total number of files and their sizes.")
	listContainerCmd.PersistentFlags().BoolVar(&raw.MegaUnits, "mega-units", false, "False by default. Displays units in orders of 1000, not 1024.")
	listContainerCmd.PersistentFlags().StringVar(&raw.Properties, "properties", "", "Properties to be displayed in list output. "+
		"\n Possible properties include: "+strings.Join(validPropertiesString(), ", ")+". "+
		"\n Delimiter (;) should be used to separate multiple values of properties (i.e. 'LastModifiedTime;VersionId;BlobType').")
	listContainerCmd.PersistentFlags().StringVar(&raw.trailingDot, "trailing-dot", "", "'Enable' by default to treat file share related operations in a safe manner. "+
		"\n Available options: "+strings.Join(common.ValidTrailingDotOptions(), ", ")+". "+
		"\n Choose 'Disable' to go back to legacy (potentially unsafe) treatment of trailing dot files where the file service will trim any trailing dots in paths. "+
		"\n This can result in potential data corruption if the transfer contains two paths that differ only by a trailing dot (ex: mypath and mypath.). "+
		"\n If this flag is set to 'Disable' and AzCopy encounters a trailing dot file, it will warn customers in the scanning log but will not attempt to abort the operation."+
		"\n If the destination does not support trailing dot files (Windows or Blob Storage), "+
		"\n AzCopy will fail if the trailing dot file is the root of the transfer and skip any trailing dot paths encountered during enumeration.")

	rootCmd.AddCommand(listContainerCmd)
}

// handleListContainerCommand handles the list container command
func (cooked cookedListCmdArgs) handleListContainerCommand() (err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	var credentialInfo common.CredentialInfo

	source, err := traverser2.SplitResourceString(cooked.sourcePath, cooked.location)
	if err != nil {
		return err
	}

	if err := common.VerifyIsURLResolvable(raw.src); cooked.location.IsRemote() && err != nil {
		return fmt.Errorf("failed to resolve target: %w", err)
	}

	level, err := DetermineLocationLevel(source.Value, cooked.location, true)
	if err != nil {
		return err
	}

	// isSource is rather misnomer for canBePublic. We can list public containers, and hence isSource=true
	uotm := Client.GetUserOAuthTokenManagerInstance()
	if credentialInfo, _, err = GetCredentialInfoForLocation(ctx, cooked.location, source, true, uotm, common.CpkOptions{}); err != nil {
		return fmt.Errorf("failed to obtain credential info: %s", err.Error())
	} else if credentialInfo.CredentialType.IsAzureOAuth() {
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else {
			credentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	// check if user wants to get version id
	getVersionId := containsProperty(cooked.properties, VersionId)

	traverser, err := traverser2.InitResourceTraverser(source, cooked.location, ctx, traverser2.InitResourceTraverserOptions{
		Credential: &credentialInfo,

		TrailingDotOption: cooked.trailingDot,

		Recursive:               true,
		GetPropertiesInFrontend: true,

		ListVersions:     getVersionId,
		HardlinkHandling: common.EHardlinkHandlingType.Follow(),
	})
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

	processor := func(object traverser2.StoredObject) error {
		lo := cooked.newListObject(object, level)
		glcm.Output(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				jsonOutput, err := json.Marshal(lo)
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else {
				return lo.String()
			}
		}, common.EOutputMessageType.ListObject())

		// ensure that versioned objects don't get counted multiple times in the tally
		// 1. only include the size of the latest version of the object in the sizeCount
		// 2. only include the object once in the fileCount
		if cooked.RunningTally {
			if getVersionId {
				// get new version id object
				updatedVersionId := versionIdObject{
					versionId: object.BlobVersionID,
					fileSize:  object.Size,
				}

				// there exists a current version id of the object
				if currentVersionId, ok := objectVer[object.RelativePath]; ok {
					// get current version id time
					currentVid, _ := time.Parse(versionIdTimeFormat, currentVersionId.versionId)

					// get new version id time
					newVid, _ := time.Parse(versionIdTimeFormat, object.BlobVersionID)

					// if new vid came after the current vid, then it is the latest version
					// update the objectVer with the latest version
					// we will also remove sizeCount and fileCount of current object, allowing
					// the updated sizeCount and fileCount to be added at line 320
					if newVid.After(currentVid) {
						sizeCount -= currentVersionId.fileSize // remove size of current object
						fileCount--                            // remove current object file count
						objectVer[object.RelativePath] = updatedVersionId
					}
				} else {
					objectVer[object.RelativePath] = updatedVersionId
				}
			}
			fileCount++
			sizeCount += object.Size
		}
		return nil
	}

	err = traverser.Traverse(nil, processor, nil)

	if err != nil {
		return fmt.Errorf("failed to Traverse container: %s", err.Error())
	}

	if cooked.RunningTally {
		ls := cooked.newListSummary(fileCount, sizeCount)
		glcm.Output(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				jsonOutput, err := json.Marshal(ls)
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else {
				return ls.String()
			}
		}, common.EOutputMessageType.ListSummary())
	}

	return nil
}

type AzCopyListObject struct {
	Path string `json:"Path"`

	LastModifiedTime *time.Time         `json:"LastModifiedTime,omitempty"`
	VersionId        string             `json:"VersionId,omitempty"`
	BlobType         blob.BlobType      `json:"BlobType,omitempty"`
	BlobAccessTier   blob.AccessTier    `json:"BlobAccessTier,omitempty"`
	ContentType      string             `json:"ContentType,omitempty"`
	ContentEncoding  string             `json:"ContentEncoding,omitempty"`
	ContentMD5       []byte             `json:"ContentMD5,omitempty"`
	LeaseState       lease.StateType    `json:"LeaseState,omitempty"`
	LeaseStatus      lease.StatusType   `json:"LeaseStatus,omitempty"`
	LeaseDuration    lease.DurationType `json:"LeaseDuration,omitempty"`
	ArchiveStatus    blob.ArchiveStatus `json:"ArchiveStatus,omitempty"`

	ContentLength string `json:"ContentLength"` // This is a string to support machine-readable

	StringEncoding string `json:"-"` // this is stored as part of the list object to avoid looping over the properties array twice
}

func (l AzCopyListObject) String() string {
	return l.StringEncoding
}

func (cooked cookedListCmdArgs) newListObject(object traverser2.StoredObject, level LocationLevel) AzCopyListObject {
	path := getPath(object.ContainerName, object.RelativePath, level, object.EntityType)
	contentLength := sizeToString(object.Size, cooked.MachineReadable)

	lo := AzCopyListObject{
		Path:          path,
		ContentLength: contentLength,
	}

	builder := strings.Builder{}
	builder.WriteString(lo.Path + "; ")

	for _, property := range cooked.properties {
		propertyStr := string(property)
		switch property {
		case LastModifiedTime:
			lo.LastModifiedTime = to.Ptr(object.LastModifiedTime)
			builder.WriteString(propertyStr + ": " + lo.LastModifiedTime.String() + "; ")
		case VersionId:
			lo.VersionId = object.BlobVersionID
			builder.WriteString(propertyStr + ": " + lo.VersionId + "; ")
		case BlobType:
			lo.BlobType = object.BlobType
			builder.WriteString(propertyStr + ": " + string(lo.BlobType) + "; ")
		case BlobAccessTier:
			lo.BlobAccessTier = object.BlobAccessTier
			builder.WriteString(propertyStr + ": " + string(lo.BlobAccessTier) + "; ")
		case ContentType:
			lo.ContentType = object.ContentType
			builder.WriteString(propertyStr + ": " + lo.ContentType + "; ")
		case ContentEncoding:
			lo.ContentEncoding = object.ContentEncoding
			builder.WriteString(propertyStr + ": " + lo.ContentEncoding + "; ")
		case ContentMD5:
			lo.ContentMD5 = object.Md5
			builder.WriteString(propertyStr + ": " + base64.StdEncoding.EncodeToString(lo.ContentMD5) + "; ")
		case LeaseState:
			lo.LeaseState = object.LeaseState
			builder.WriteString(propertyStr + ": " + string(lo.LeaseState) + "; ")
		case LeaseStatus:
			lo.LeaseStatus = object.LeaseStatus
			builder.WriteString(propertyStr + ": " + string(lo.LeaseStatus) + "; ")
		case LeaseDuration:
			lo.LeaseDuration = object.LeaseDuration
			builder.WriteString(propertyStr + ": " + string(lo.LeaseDuration) + "; ")
		case ArchiveStatus:
			lo.ArchiveStatus = object.ArchiveStatus
			builder.WriteString(propertyStr + ": " + string(lo.ArchiveStatus) + "; ")
		}
	}
	builder.WriteString("Content Length: " + lo.ContentLength)
	lo.StringEncoding = builder.String()

	return lo
}

type AzCopyListSummary struct {
	FileCount     string `json:"FileCount"`
	TotalFileSize string `json:"TotalFileSize"`

	StringEncoding string `json:"-"`
}

func (l AzCopyListSummary) String() string {
	return l.StringEncoding
}

func (cooked cookedListCmdArgs) newListSummary(fileCount, totalFileSize int64) AzCopyListSummary {
	fc := strconv.Itoa(int(fileCount))
	tfs := sizeToString(totalFileSize, cooked.MachineReadable)

	output := "\nFile count: " + fc + "\nTotal file size: " + tfs
	return AzCopyListSummary{
		FileCount:      fc,
		TotalFileSize:  tfs,
		StringEncoding: output,
	}
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

func ByteSizeToString(size int64) string {
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

func getPath(containerName, relativePath string, level LocationLevel, entityType common.EntityType) string {
	builder := strings.Builder{}
	if level == level.Service() {
		builder.WriteString(containerName + "/")
	}
	builder.WriteString(relativePath)
	if entityType == common.EEntityType.Folder() && !strings.HasSuffix(relativePath, "/") {
		builder.WriteString("/")
	}
	return builder.String()
}

func sizeToString(size int64, machineReadable bool) string {
	return common.Iff(machineReadable, strconv.Itoa(int(size)), ByteSizeToString(size))
}
