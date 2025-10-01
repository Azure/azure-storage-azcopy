// Copyright © 2025 Microsoft <azcopydev@microsoft.com>
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
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func (u *azureFileSenderBase) prepareNFSPropertiesFull() *file.NFSProperties {
	return &file.NFSProperties{
		CreationTime:  u.nfsPropertiesToApply.CreationTime,
		LastWriteTime: u.nfsPropertiesToApply.LastWriteTime,
		Owner:         u.nfsPropertiesToApply.Owner,
		Group:         u.nfsPropertiesToApply.Group,
		FileMode:      u.nfsPropertiesToApply.FileMode,
	}
}

func (u *azureFileSenderBase) prepareNFSProperties(creation, lastWrite *time.Time) *file.NFSProperties {
	return &file.NFSProperties{
		CreationTime:  creation,
		LastWriteTime: lastWrite,
	}
}

func (u *azureFileSenderBase) prepareSMBProperties(creation, lastWrite *time.Time) *file.SMBProperties {
	return &file.SMBProperties{
		CreationTime:  creation,
		LastWriteTime: lastWrite,
	}
}

func (u *azureFileSenderBase) applyNFSHeaders(info *TransferInfo) error {
	if stage, err := u.addNFSPropertiesToHeaders(info); err != nil {
		u.jptm.FailActiveSend(stage, err)
		return err
	}
	if stage, err := u.addNFSPermissionsToHeaders(info, u.getFileClient().URL()); err != nil {
		u.jptm.FailActiveSend(stage, err)
		return err
	}
	return nil
}

func (u *azureFileSenderBase) applySMBHeaders(info *TransferInfo) error {
	if stage, err := u.addPermissionsToHeaders(info, u.getFileClient().URL()); err != nil {
		u.jptm.FailActiveSend(stage, err)
		return err
	}
	if stage, err := u.addSMBPropertiesToHeaders(info); err != nil {
		u.jptm.FailActiveSend(stage, err)
		return err
	}
	return nil
}

func (u *azureFileSenderBase) addCreationOptions(createOptions *file.CreateOptions) {
	jptm := u.jptm
	info := jptm.Info()

	switch {
	// Cross-protocol transfers (SMB <-> NFS)
	case jptm.FromTo() == common.EFromTo.FileNFSFileSMB(),
		jptm.FromTo() == common.EFromTo.FileSMBFileNFS():

		creationTime, lastWriteTime := u.getPropertiesForCrossProtocolTransfer()
		if jptm.FromTo().To() == common.ELocation.FileNFS() {
			createOptions.NFSProperties = u.prepareNFSProperties(creationTime, lastWriteTime)
		} else {
			createOptions.SMBProperties = u.prepareSMBProperties(creationTime, lastWriteTime)
		}

	// Pure NFS case
	case jptm.FromTo().IsNFS():
		if err := u.applyNFSHeaders(info); err != nil {
			return
		}
		createOptions.NFSProperties = u.prepareNFSPropertiesFull()

	// Default SMB case
	default:
		if err := u.applySMBHeaders(info); err != nil {
			return
		}
		createOptions.SMBProperties = &u.smbPropertiesToApply
		createOptions.Permissions = &u.permissionsToApply
	}
}

// for cross-protocol transfer, we need to set properties(creationTime, lastWriteTime)
// only as permissions are not supported.
func (u *azureFileSenderBase) getPropertiesForCrossProtocolTransfer() (creationTime, lastWriteTime *time.Time) {
	jptm := u.jptm
	info := jptm.Info()

	var err error
	var stage string

	if jptm.FromTo().From() == common.ELocation.FileNFS() {
		stage, err = u.addNFSPropertiesToHeaders(info)
		if err == nil {
			return u.nfsPropertiesToApply.CreationTime, u.nfsPropertiesToApply.LastWriteTime
		}
	} else {
		stage, err = u.addSMBPropertiesToHeaders(info)
		if err == nil {
			return u.smbPropertiesToApply.CreationTime, u.smbPropertiesToApply.LastWriteTime
		}
	}

	if err != nil {
		jptm.FailActiveSend(stage, err)
	}
	return nil, nil
}

func (u *azureFileSenderBase) addNFSPropertiesToHeaders(info *TransferInfo) (stage string, err error) {
	if !info.PreserveInfo {
		return "", nil
	}
	if nfsSIP, ok := u.sip.(INFSPropertyBearingSourceInfoProvider); ok {
		nfsProps, err := nfsSIP.GetNFSProperties()
		if err != nil {
			return "Obtaining NFS properties", err
		}

		if info.ShouldTransferLastWriteTime() {
			lwTime := nfsProps.FileLastWriteTime()
			u.nfsPropertiesToApply.LastWriteTime = &lwTime
		}

		creationTime := nfsProps.FileCreationTime()
		u.nfsPropertiesToApply.CreationTime = &creationTime
	}
	return "", nil
}

func (u *azureFileSenderBase) addNFSPermissionsToHeaders(info *TransferInfo, destURL string) (stage string, err error) {
	if !info.PreservePermissions.IsTruthy() {
		if nfsSIP, ok := u.sip.(INFSPropertyBearingSourceInfoProvider); ok {
			fileMode, owner, group, err := nfsSIP.GetNFSDefaultPerms()
			if err != nil {
				return "Obtaining NFS default permissions", err
			}
			u.nfsPropertiesToApply.Owner = owner
			u.nfsPropertiesToApply.Group = group
			u.nfsPropertiesToApply.FileMode = fileMode
		}
		return "", nil
	}

	if nfsSIP, ok := u.sip.(INFSPropertyBearingSourceInfoProvider); ok {
		nfsPerms, err := nfsSIP.GetNFSPermissions()
		if err != nil {
			return "Obtaining NFS permissions", err
		}
		u.nfsPropertiesToApply.Owner = nfsPerms.GetOwner()
		u.nfsPropertiesToApply.Group = nfsPerms.GetGroup()
		u.nfsPropertiesToApply.FileMode = nfsPerms.GetFileMode()
	}
	return "", nil
}

func (u *azureFileSenderBase) addPermissionsToHeaders(info *TransferInfo, destURL string) (stage string, err error) {
	if !info.PreservePermissions.IsTruthy() {
		return "", nil
	}

	// Prepare to transfer SDDLs from the source.
	if sddlSIP, ok := u.sip.(ISMBPropertyBearingSourceInfoProvider); ok {
		// If both sides are Azure Files...
		if fSIP, ok := sddlSIP.(*fileSourceInfoProvider); ok {

			srcURLParts, err := file.ParseURL(info.Source)
			common.PanicIfErr(err)
			dstURLParts, err := file.ParseURL(destURL)
			common.PanicIfErr(err)

			// and happen to be the same account and share, we can get away with using the same key and save a trip.
			if srcURLParts.Host == dstURLParts.Host && srcURLParts.ShareName == dstURLParts.ShareName {
				u.permissionsToApply.PermissionKey = &fSIP.cachedPermissionKey
			}
		}

		// If we didn't do the workaround, then let's get the SDDL and put it later.
		if u.permissionsToApply.PermissionKey == nil || *u.permissionsToApply.PermissionKey == "" {
			pString, err := sddlSIP.GetSDDL()

			// Sending "" to the service is invalid, but the service will return it sometimes (e.g. on file shares)
			// Thus, we'll let the files SDK fill in "inherit" for us, so the service is happy.
			if pString != "" {
				u.permissionsToApply.Permission = &pString
			}

			if err != nil {
				return "Getting permissions", err
			}
		}
	}

	if u.permissionsToApply.Permission != nil && len(*u.permissionsToApply.Permission) > FilesServiceMaxSDDLSize {
		sipm := u.jptm.SecurityInfoPersistenceManager()
		pkey, err := sipm.PutSDDL(*u.permissionsToApply.Permission, u.shareClient)
		u.permissionsToApply.PermissionKey = &pkey
		if err != nil {
			return "Putting permissions", err
		}

		// At this point, we’ve stored the full SDDL string in the security persistence manager
		// and obtained a PermissionKey that references it.  
		// To avoid sending an oversized SDDL string,  
		// we replace the Permission field with an empty string.  
		// This ensures that only the PermissionKey is sent to the service, not the large SDDL itself.
		ePermString := ""
		u.permissionsToApply.Permission = &ePermString
	}
	return "", nil
}

func (u *azureFileSenderBase) addSMBPropertiesToHeaders(info *TransferInfo) (stage string, err error) {
	if !info.PreserveInfo {
		return "", nil
	}
	if smbSIP, ok := u.sip.(ISMBPropertyBearingSourceInfoProvider); ok {
		smbProps, err := smbSIP.GetSMBProperties()

		if err != nil {
			return "Obtaining SMB properties", err
		}

		fromTo := u.jptm.FromTo()
		if fromTo.From() == common.ELocation.File() { // Files SDK can panic when the service hands it something unexpected!
			defer func() { // recover from potential panics and output raw properties for debug purposes
				if panicerr := recover(); panicerr != nil {
					stage = "Reading SMB properties"

					attr, _ := smbProps.FileAttributes()
					lwt := smbProps.FileLastWriteTime()
					fct := smbProps.FileCreationTime()

					err = fmt.Errorf("failed to read SMB properties (%w)! Raw data: attr: `%s` lwt: `%s`, fct: `%s`", err, attr, lwt, fct)
				}
			}()
		}

		attribs, _ := smbProps.FileAttributes()
		u.smbPropertiesToApply.Attributes = attribs

		if info.ShouldTransferLastWriteTime() {
			lwTime := smbProps.FileLastWriteTime()
			u.smbPropertiesToApply.LastWriteTime = &lwTime
		}

		creationTime := smbProps.FileCreationTime()
		u.smbPropertiesToApply.CreationTime = &creationTime
	}
	return "", nil
}

func (u *azureFileSenderBase) buildSetHTTPHeadersOptions() *file.SetHTTPHeadersOptions {
	// Base always includes headers
	opts := &file.SetHTTPHeadersOptions{
		HTTPHeaders: &u.headersToApply,
	}

	switch {
	// Cross-protocol transfers
	case u.jptm.FromTo() == common.EFromTo.FileNFSFileSMB():
		opts.SMBProperties = u.prepareSMBProperties(u.nfsPropertiesToApply.CreationTime,
			u.nfsPropertiesToApply.LastWriteTime)

	case u.jptm.FromTo() == common.EFromTo.FileSMBFileNFS():
		opts.NFSProperties = u.prepareNFSProperties(u.smbPropertiesToApply.CreationTime,
			u.smbPropertiesToApply.LastWriteTime)
	// Pure NFS → NFS
	case u.jptm.FromTo().IsNFS():
		opts.NFSProperties = u.prepareNFSPropertiesFull()
	// Default SMB → SMB
	default:
		opts.Permissions = &u.permissionsToApply
		opts.SMBProperties = &u.smbPropertiesToApply
	}
	return opts
}
