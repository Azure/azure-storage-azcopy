//go:build windows

package e2etest

import (
	"fmt"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/sddl"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/hillu/go-ntdll"
	"golang.org/x/sys/windows"
)

// getHandle obtains a windows file handle with generic read permissions & backup semantics
func (l LocalObjectResourceManager) getHandle(path string, a Asserter) ntdll.Handle {
	srcPtr, err := syscall.UTF16PtrFromString(path)
	a.NoError("Get UTF16 pointer", err)

	// custom open call, because must specify FILE_FLAG_BACKUP_SEMANTICS to make --backup mode work properly (i.e. our use of SeBackupPrivilege)
	fd, err := windows.CreateFile(srcPtr,
		windows.GENERIC_READ, windows.FILE_SHARE_READ, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	a.NoError("Get file descriptor", err)

	return ntdll.Handle(fd)
}

func (l LocalObjectResourceManager) closeHandle(handle ntdll.Handle, a Asserter) {
	err := windows.CloseHandle(windows.Handle(handle))
	a.NoError("Close handle", err)
}

func (l LocalObjectResourceManager) GetSDDL(a Asserter) string {
	filePath := l.getWorkingPath()
	fd := l.getHandle(filePath, a)
	defer l.closeHandle(fd, a)

	buf := make([]byte, 512)
	bufLen := uint32(len(buf))
	needValidate := false
	status := ntdll.CallWithExpandingBuffer(func() ntdll.NtStatus {
		status := ntdll.NtQuerySecurityObject(
			fd,
			windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
			(*ntdll.SecurityDescriptor)(unsafe.Pointer(&buf[0])),
			uint32(len(buf)),
			&bufLen)

		if status == ntdll.STATUS_SUCCESS {
			sd := (*windows.SECURITY_DESCRIPTOR)(unsafe.Pointer(&buf[0]))

			bufLen = sd.Length()
			needValidate = true
		}

		return status
	}, &buf, &bufLen)

	if status != ntdll.STATUS_SUCCESS {
		a.Error(fmt.Sprintf("failed to query security object %s (ntstatus: %s)", filePath, status.String()))
	}

	sd := (*windows.SECURITY_DESCRIPTOR)(unsafe.Pointer(&buf[0])) // ntdll.SecurityDescriptor is equivalent
	if needValidate && !sd.IsValid() {
		a.Error(fmt.Sprintf("failed to query security object %s (invalid descriptor returned with a successful status)", filePath))
	}

	fSDDL, err := sddl.ParseSDDL(sd.String())
	if err != nil {
		a.NoError(fmt.Sprintf("failed to parse SDDL for security object %s", filePath), err)
	}

	a.AssertNow("SDDL sanity check", Equal{}, fSDDL.String(), sd.String())
	return fSDDL.PortableString()
}

func (l LocalObjectResourceManager) GetSMBProperties(a Asserter) ste.TypedSMBPropertyHolder {
	info, err := common.GetFileInformation(l.getWorkingPath())
	a.NoError("get file SMB props", err)

	return ste.HandleInfo{ByHandleFileInformation: info}
}

func (l LocalObjectResourceManager) PutSMBProperties(a Asserter, properties FileProperties) {
	filePath := l.getWorkingPath()
	pathPtr, err := syscall.UTF16PtrFromString(filePath)
	a.NoError("get UTF16 pointer for path", err)

	if properties.FileAttributes != nil {
		attr, err := ParseNTFSAttributes(*properties.FileAttributes)
		a.NoError("Parse attributes", err)

		err = windows.SetFileAttributes(pathPtr, uint32(attr))
		a.NoError("Set file attributes", err)
	}

	if properties.hasCustomTimes() {
		var sa windows.SecurityAttributes
		sa.Length = uint32(unsafe.Sizeof(sa))
		sa.InheritHandle = 1

		var creation, lastWrite *windows.Filetime

		if properties.FileCreationTime != nil {
			c := windows.NsecToFiletime(properties.FileCreationTime.UnixNano())
			creation = &c
		}

		if properties.FileLastWriteTime != nil {
			lmt := windows.NsecToFiletime(properties.FileLastWriteTime.UnixNano())
			lastWrite = &lmt
		}

		// need custom CreateFile call because need FILE_WRITE_ATTRIBUTES
		fd, err := windows.CreateFile(pathPtr,
			windows.FILE_WRITE_ATTRIBUTES, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, &sa,
			windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
		a.NoError(fmt.Sprintf("Get file descriptor %s", filePath), err)
		defer a.NoError("close handle", windows.Close(fd))

		err = windows.SetFileTime(fd, creation, nil, lastWrite)
		a.NoError("Set file times", err)
	}
}

// These syscalls are unfortunately, thread-sensitive.
var globalSetAclMu = &sync.Mutex{}

// PutSDDL sets SDDLs like AzCopy does for downloads
func (l LocalObjectResourceManager) PutSDDL(sddlstr string, a Asserter) {
	filePath := l.getWorkingPath()

	sd, err := windows.SecurityDescriptorFromString(sddlstr)
	a.NoError("parse security descriptor", err)

	parsed, err := sddl.ParseSDDL(sddlstr)
	a.NoError("parse security descriptor (internal lib)", err)

	ctl, _, err := sd.Control()
	a.NoError("Get control bits", err)

	var securityInfoFlags windows.SECURITY_INFORMATION = windows.DACL_SECURITY_INFORMATION

	parsedSDDL, err := sddl.ParseSDDL(sddlstr)

	isProtectedAtSource := (ctl & windows.SE_DACL_PROTECTED) != 0
	// treat rawpath like it's always the root
	isAtTransferRoot := l.rawPath != "" || l.objectPath == ""

	/*
		via Jason Shay:
		One exception is related to the "AI" flag.
		If you provide a descriptor to NtSetSecurityObject with just AI (SE_DACL_AUTO_INHERITED), it will not be stored.
		If you provide it with SE_DACL_AUTO_INHERITED AND SE_DACL_AUTO_INHERIT_REQ, then SE_DACL_AUTO_INHERITED will be stored (note the _REQ flag is never stored)

		The REST API for Azure Files will see the "AI" in the SDDL, and will do the _REQ flag work in the background for you.
	*/
	if strings.Contains(parsedSDDL.DACL.Flags, "AI") {
		// set the DACL auto-inherit flag, since Windows didn't pick it up for some reason...
		err := sd.SetControl(windows.SE_DACL_AUTO_INHERITED|windows.SE_DACL_AUTO_INHERIT_REQ, windows.SE_DACL_AUTO_INHERITED|windows.SE_DACL_AUTO_INHERIT_REQ)
		a.NoError("tried to persist auto-inherit bit", err)
	}

	// Protect the root so that we don't have parent ACLs affect the rest of the transfer
	if isProtectedAtSource || isAtTransferRoot {
		securityInfoFlags |= windows.PROTECTED_DACL_SECURITY_INFORMATION
	}

	if parsed.GroupSID != "" {
		securityInfoFlags |= windows.GROUP_SECURITY_INFORMATION
	}

	if parsed.OwnerSID != "" {
		securityInfoFlags |= windows.OWNER_SECURITY_INFORMATION
	}

	globalSetAclMu.Lock()
	defer globalSetAclMu.Unlock()

	destPtr, err := syscall.UTF16PtrFromString(filePath)
	a.NoError("Get UTF16 ptr", err)

	var sa windows.SecurityAttributes
	sa.Length = uint32(unsafe.Sizeof(sa))
	sa.InheritHandle = 1

	// need WRITE_DAC and WRITE_OWNER for SDDL preservation, no need for ACCESS_SYSTEM_SECURITY, since we don't back up SACLs.
	fd, err := windows.CreateFile(destPtr, windows.FILE_GENERIC_WRITE|windows.WRITE_DAC|windows.WRITE_OWNER, windows.FILE_SHARE_WRITE, &sa,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	a.NoError("Open file descriptor", err)
	defer a.NoError("Close file descriptor", windows.Close(fd))

	status := ntdll.NtSetSecurityObject(
		ntdll.Handle(fd),
		ntdll.SecurityInformationT(securityInfoFlags),
		// unsafe but actually safe conversion
		(*ntdll.SecurityDescriptor)(unsafe.Pointer(sd)),
	)
	a.Assert("Set ACLs status must be successful", Equal{}, status, ntdll.STATUS_SUCCESS)
}

// TODO: Add NFS handling for windows later
func (l LocalObjectResourceManager) GetNFSProperties(a Asserter) ste.TypedNFSPropertyHolder {
	return nil
}

// TODO: Add NFS handling for windows later
func (l LocalObjectResourceManager) GetNFSPermissions(a Asserter) ste.TypedNFSPermissionsHolder {
	return nil
}

// TODO: Add NFS handling for windows later
func (l LocalObjectResourceManager) PutNFSProperties(a Asserter, properties FileNFSProperties) {
	return
}

// TODO: Add NFS handling for windows later
func (l LocalObjectResourceManager) PutNFSPermissions(a Asserter, permissions FileNFSPermissions) {
	return
}

// Add support later when we support windows for NFS
func CreateSpecialFile(a Asserter, filepath string) error {
	return nil
}
