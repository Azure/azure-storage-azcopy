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

package ste

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/common"
)

type URLHolder interface {
	URL() url.URL
	String() string
}

// azureFileSenderBase implements both IFolderSender and (most of) IFileSender.
// Why implement both interfaces in the one type, even though they are largely unrelated? Because it
// makes functions like newAzureFilesUploader easier to reason about, since they always return the same type.
// It may also make it easier to describe what's needed when supporting an new backend - e.g. "to send to a new back end
// you need a sender that implements IFileSender and, if the back end is folder aware, it should also implement IFolderSender"
// (The alternative would be to have the likes of newAzureFilesUploader call sip.EntityType and return a different type
// if the entity type is folder).
type azureFileSenderBase struct {
	jptm         IJobPartTransferMgr
	fileOrDirURL URLHolder
	chunkSize    int64
	numChunks    uint32
	pipeline     pipeline.Pipeline
	pacer        pacer
	ctx          context.Context
	sip          ISourceInfoProvider
	// Headers and other info that we will apply to the destination
	// object. For S2S, these come from the source service.
	// When sending local data, they are computed based on
	// the properties of the local file
	headersToApply  azfile.FileHTTPHeaders
	metadataToApply azfile.Metadata
}

func newAzureFileSenderBase(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (*azureFileSenderBase, error) {

	info := jptm.Info()

	// compute chunk size (irrelevant but harmless for folders)
	// If the given chunk Size for the Job is greater than maximum file chunk size i.e 4 MB
	// then chunk size will be 4 MB.
	chunkSize := info.BlockSize
	if chunkSize > common.DefaultAzureFileChunkSize {
		chunkSize = common.DefaultAzureFileChunkSize
		if jptm.ShouldLog(pipeline.LogWarning) {
			jptm.Log(pipeline.LogWarning,
				fmt.Sprintf("Block size %d larger than maximum file chunk size, 4 MB chunk size used", info.BlockSize))
		}
	}

	// compute num chunks (irrelevant but harmless for folders)
	numChunks := getNumChunks(info.SourceSize, chunkSize)

	// make sure URL is parsable
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	// due to the REST parity feature added in 2019-02-02, the File APIs are no longer backward compatible
	// so we must use the latest SDK version to stay safe
	ctx := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azfile.ServiceVersion)

	props, err := sip.Properties()
	if err != nil {
		return nil, err
	}

	var h URLHolder
	if info.IsFolderPropertiesTransfer() {
		h = azfile.NewDirectoryURL(*destURL, p)
	} else {
		h = azfile.NewFileURL(*destURL, p)
	}

	return &azureFileSenderBase{
		jptm:            jptm,
		fileOrDirURL:    h,
		chunkSize:       chunkSize,
		numChunks:       numChunks,
		pipeline:        p,
		pacer:           pacer,
		ctx:             ctx,
		headersToApply:  props.SrcHTTPHeaders.ToAzFileHTTPHeaders(),
		sip:             sip,
		metadataToApply: props.SrcMetadata.ToAzFileMetadata(),
	}, nil
}

func (u *azureFileSenderBase) fileURL() azfile.FileURL {
	return u.fileOrDirURL.(azfile.FileURL)
}

func (u *azureFileSenderBase) dirURL() azfile.DirectoryURL {
	return u.fileOrDirURL.(azfile.DirectoryURL)
}

func (u *azureFileSenderBase) ChunkSize() int64 {
	return u.chunkSize
}

func (u *azureFileSenderBase) NumChunks() uint32 {
	return u.numChunks
}

func (u *azureFileSenderBase) RemoteFileExists() (bool, time.Time, error) {
	return remoteObjectExists(u.fileURL().GetProperties(u.ctx))
}

func (u *azureFileSenderBase) Prologue(state common.PrologueState) (destinationModified bool) {
	jptm := u.jptm
	info := jptm.Info()

	destinationModified = true

	// Create the parent directories of the file. Note share must be existed, as the files are listed from share or directory.
	err := AzureFileParentDirCreator{}.CreateParentDirToRoot(u.ctx, u.fileURL(), u.pipeline, u.jptm.GetFolderCreationTracker())
	if err != nil {
		jptm.FailActiveUpload("Creating parent directory", err)
		return
	}

	// sometimes, specifically when reading local files, we have more info
	// about the file type at this time than what we had before
	u.headersToApply.ContentType = state.GetInferredContentType(u.jptm)

	stage, err := u.addPermissionsToHeaders(info, u.fileURL().URL())
	if err != nil {
		jptm.FailActiveSend(stage, err)
		return
	}

	stage, err = u.addSMBPropertiesToHeaders(info, u.fileURL().URL())
	if err != nil {
		jptm.FailActiveSend(stage, err)
		return
	}

	// Turn off readonly at creation time (because if its set at creation time, we won't be
	// able to upload any data to the file!). We'll set it in epilogue, if necessary.
	creationHeaders := u.headersToApply
	if creationHeaders.FileAttributes != nil {
		revisedAttribs := creationHeaders.FileAttributes.Remove(azfile.FileAttributeReadonly)
		creationHeaders.FileAttributes = &revisedAttribs
	}

	err = u.DoWithOverrideReadOnly(u.ctx,
		func() (interface{}, error) {
			return u.fileURL().Create(u.ctx, info.SourceSize, creationHeaders, u.metadataToApply)
		},
		u.fileOrDirURL,
		u.jptm.GetForceIfReadOnly())
	if err != nil {
		jptm.FailActiveUpload("Creating file", err)
		return
	}

	return
}

// DoWithOverrideReadOnly performs the given action, and forces it to happen even if the target is read only.
// NOTE that all SMB attributes (and other headers?) on the target will be lost, so only use this if you don't need them any more
// (e.g. you are about to delete the resource, or you are going to reset the attributes/headers)
func (*azureFileSenderBase) DoWithOverrideReadOnly(ctx context.Context, action func() (interface{}, error), targetFileOrDir URLHolder, enableForcing bool) error {
	// try the action
	_, err := action()

	failedAsReadOnly := false
	if strErr, ok := err.(azfile.StorageError); ok && strErr.ServiceCode() == azfile.ServiceCodeReadOnlyAttribute {
		failedAsReadOnly = true
	}
	if !failedAsReadOnly {
		return err
	}

	// did fail as readonly, but forcing is not enabled
	if !enableForcing {
		return errors.New("target is readonly. To force the action to proceed, add --force-if-read-only to the command line")
	}

	// did fail as readonly, and forcing is enabled
	none := azfile.FileAttributeNone
	if f, ok := targetFileOrDir.(azfile.FileURL); ok {
		h := azfile.FileHTTPHeaders{}
		h.FileAttributes = &none // clear the attribs
		_, err = f.SetHTTPHeaders(ctx, h)
	} else if d, ok := targetFileOrDir.(azfile.DirectoryURL); ok {
		// this code path probably isn't used, since ReadOnly (in Windows file systems at least)
		// only applies to the files in a folder, not to the folder itself. But we'll leave the code here, for now.
		_, err = d.SetProperties(ctx, azfile.SMBProperties{FileAttributes: &none})
	} else {
		err = errors.New("cannot remove read-only attribute from unknown target type")
	}
	if err != nil {
		return err
	}

	// retry the action
	_, err = action()
	return err
}

func (u *azureFileSenderBase) addPermissionsToHeaders(info TransferInfo, destUrl url.URL) (stage string, err error) {
	if !info.PreserveSMBPermissions.IsTruthy() {
		return "", nil
	}

	// Prepare to transfer SDDLs from the source.
	if sddlSIP, ok := u.sip.(ISMBPropertyBearingSourceInfoProvider); ok {
		// If both sides are Azure Files...
		if fSIP, ok := sddlSIP.(*fileSourceInfoProvider); ok {
			srcURL, err := url.Parse(info.Source)
			common.PanicIfErr(err)

			srcURLParts := azfile.NewFileURLParts(*srcURL)
			dstURLParts := azfile.NewFileURLParts(destUrl)

			// and happen to be the same account and share, we can get away with using the same key and save a trip.
			if srcURLParts.Host == dstURLParts.Host && srcURLParts.ShareName == dstURLParts.ShareName {
				u.headersToApply.PermissionKey = &fSIP.cachedPermissionKey
			}
		}

		// If we didn't do the workaround, then let's get the SDDL and put it later.
		if u.headersToApply.PermissionKey == nil || *u.headersToApply.PermissionKey == "" {
			pString, err := sddlSIP.GetSDDL()
			u.headersToApply.PermissionString = &pString
			if err != nil {
				return "Getting permissions", err
			}
		}
	}

	if len(*u.headersToApply.PermissionString) > filesServiceMaxSDDLSize {
		fURLParts := azfile.NewFileURLParts(destUrl)
		fURLParts.DirectoryOrFilePath = ""
		shareURL := azfile.NewShareURL(fURLParts.URL(), u.pipeline)

		sipm := u.jptm.SecurityInfoPersistenceManager()
		pkey, err := sipm.PutSDDL(*u.headersToApply.PermissionString, shareURL)
		u.headersToApply.PermissionKey = &pkey
		if err != nil {
			return "Putting permissions", err
		}

		ePermString := ""
		u.headersToApply.PermissionString = &ePermString
	}
	return "", nil
}

func (u *azureFileSenderBase) addSMBPropertiesToHeaders(info TransferInfo, destUrl url.URL) (stage string, err error) {
	if !info.PreserveSMBInfo {
		return "", nil
	}
	if smbSIP, ok := u.sip.(ISMBPropertyBearingSourceInfoProvider); ok {
		smbProps, err := smbSIP.GetSMBProperties()

		if err != nil {
			return "Obtaining SMB properties", err
		}

		attribs := smbProps.FileAttributes()
		u.headersToApply.FileAttributes = &attribs

		if info.ShouldTransferLastWriteTime() {
			lwTime := smbProps.FileLastWriteTime()
			u.headersToApply.FileLastWriteTime = &lwTime
		}

		creationTime := smbProps.FileCreationTime()
		u.headersToApply.FileCreationTime = &creationTime
	}
	return "", nil
}

func (u *azureFileSenderBase) Epilogue() {
	// when readonly=true we deliberately omit it a creation time, so must set it here
	resendReadOnly := u.headersToApply.FileAttributes != nil &&
		u.headersToApply.FileAttributes.Has(azfile.FileAttributeReadonly)

	// when archive bit is false, it must be set in a separate call (like we do here). As at March 2020,
	// the Service does not respect attempts to set it to false at time of creating the file.
	resendArchive := u.headersToApply.FileAttributes != nil &&
		u.headersToApply.FileAttributes.Has(azfile.FileAttributeArchive) == false

	if u.jptm.IsLive() && (resendReadOnly || resendArchive) && u.jptm.Info().PreserveSMBInfo {
		//This is an extra round trip, but we can live with that for these relatively rare cases
		_, err := u.fileURL().SetHTTPHeaders(u.ctx, u.headersToApply)
		if err != nil {
			u.jptm.FailActiveSend("Applying final attribute settings", err)
		}
	}
}

func (u *azureFileSenderBase) Cleanup() {
	jptm := u.jptm

	// Cleanup
	if jptm.IsDeadInflight() {
		// transfer was either failed or cancelled
		// the file created in share needs to be deleted, since it's
		// contents will be at an unknown stage of partial completeness
		deletionContext, cancelFn := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancelFn()
		_, err := u.fileURL().Delete(deletionContext)
		if err != nil {
			jptm.Log(pipeline.LogError, fmt.Sprintf("error deleting the (incomplete) file %s. Failed with error %s", u.fileOrDirURL.String(), err.Error()))
		}
	}
}

func (u *azureFileSenderBase) GetDestinationLength() (int64, error) {
	prop, err := u.fileURL().GetProperties(u.ctx)

	if err != nil {
		return -1, err
	}

	return prop.ContentLength(), nil
}

func (u *azureFileSenderBase) EnsureFolderExists() error {
	return AzureFileParentDirCreator{}.CreateDirToRoot(u.ctx, u.dirURL(), u.pipeline, u.jptm.GetFolderCreationTracker())
}

func (u *azureFileSenderBase) SetFolderProperties() error {
	info := u.jptm.Info()

	_, err := u.addPermissionsToHeaders(info, u.dirURL().URL())
	if err != nil {
		return err
	}

	_, err = u.addSMBPropertiesToHeaders(info, u.dirURL().URL())
	if err != nil {
		return err
	}

	_, err = u.dirURL().SetMetadata(u.ctx, u.metadataToApply)
	if err != nil {
		return err
	}

	err = u.DoWithOverrideReadOnly(u.ctx,
		func() (interface{}, error) { return u.dirURL().SetProperties(u.ctx, u.headersToApply.SMBProperties) },
		u.fileOrDirURL,
		u.jptm.GetForceIfReadOnly())
	return err
}

// namespace for functions related to creating parent directories in Azure File
// to avoid free floating global funcs
type AzureFileParentDirCreator struct{}

// getParentDirectoryURL gets parent directory URL of an Azure FileURL.
func (AzureFileParentDirCreator) getParentDirectoryURL(uh URLHolder, p pipeline.Pipeline) azfile.DirectoryURL {
	u := uh.URL()
	u.Path = u.Path[:strings.LastIndex(u.Path, "/")]
	return azfile.NewDirectoryURL(u, p)
}

// verifyAndHandleCreateErrors handles create errors, StatusConflict is ignored, as specific level directory could be existing.
// Report http.StatusForbidden, as user should at least have read and write permission of the destination,
// and there is no permission on directory level, i.e. create directory is a general permission for each level directories for Azure file.
func (AzureFileParentDirCreator) verifyAndHandleCreateErrors(err error) error {
	if err != nil {
		sErr, sErrOk := err.(azfile.StorageError)
		if sErrOk && sErr.Response() != nil &&
			(sErr.Response().StatusCode == http.StatusConflict) { // Note the ServiceCode actually be AuthenticationFailure when share failed to be created, if want to create share as well.
			return nil
		}
		return err
	}

	return nil
}

// splitWithoutToken splits string with a given token, and returns splitted results without token.
func (AzureFileParentDirCreator) splitWithoutToken(str string, token rune) []string {
	return strings.FieldsFunc(str, func(c rune) bool {
		return c == token
	})
}

// CreateParentDirToRoot creates parent directories of the Azure file if file's parent directory doesn't exist.
func (d AzureFileParentDirCreator) CreateParentDirToRoot(ctx context.Context, fileURL azfile.FileURL, p pipeline.Pipeline, t common.FolderCreationTracker) error {
	dirURL := d.getParentDirectoryURL(fileURL, p)
	return d.CreateDirToRoot(ctx, dirURL, p, t)
}

// CreateDirToRoot Creates the dir (and parents as necessary) if it does not exist
func (d AzureFileParentDirCreator) CreateDirToRoot(ctx context.Context, dirURL azfile.DirectoryURL, p pipeline.Pipeline, t common.FolderCreationTracker) error {
	dirURLExtension := common.FileURLPartsExtension{FileURLParts: azfile.NewFileURLParts(dirURL.URL())}
	if _, err := dirURL.GetProperties(ctx); err != nil {
		if resp, respOk := err.(pipeline.Response); respOk && resp.Response() != nil &&
			(resp.Response().StatusCode == http.StatusNotFound ||
				resp.Response().StatusCode == http.StatusForbidden) {
			// Either the parent directory does not exist, or we may not have read permissions.
			// Try to create the parent directories. Split directories as segments.
			segments := d.splitWithoutToken(dirURLExtension.DirectoryOrFilePath, '/')

			shareURL := azfile.NewShareURL(dirURLExtension.GetShareURL(), p)
			curDirURL := shareURL.NewRootDirectoryURL() // Share directory should already exist, doesn't support creating share
			// Try to create the directories
			for i := 0; i < len(segments); i++ {
				curDirURL = curDirURL.NewDirectoryURL(segments[i])
				// TODO: Persist permissions on folders.
				_, err := curDirURL.Create(ctx, azfile.Metadata{}, azfile.SMBProperties{})
				if err == nil {
					// We did create it, so record that fact. I.e. THIS job created the folder.
					// Must do it here, in the routine that is shared by both the folder and the file code,
					// because due to the parallelism of AzCopy, we don't know which will get here first, file code, or folder code.
					dirUrl := curDirURL.URL()
					t.RecordCreation(dirUrl.String())
				}
				if verifiedErr := d.verifyAndHandleCreateErrors(err); verifiedErr != nil {
					return verifiedErr
				}
			}
		} else {
			return err
		}
	}

	// Directly return if parent directory exists.
	return nil
}
