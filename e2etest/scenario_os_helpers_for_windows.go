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

// TODO this file was forked from the cmd package, it needs to cleaned to keep only the necessary part

package e2etest

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/hillu/go-ntdll"
	"golang.org/x/sys/windows"
)

type osScenarioHelper struct{}

// set file attributes to test file
func (osScenarioHelper) setAttributesForLocalFile(filePath string, attrList []string) error { //nolint:golint,unused
	lpFilePath, err := syscall.UTF16PtrFromString(filePath)
	if err != nil {
		return err
	}

	fileAttributeMap := map[string]uint32{
		"R": 1,
		"A": 32,
		"S": 4,
		"H": 2,
		"C": 2048,
		"N": 128,
		"E": 16384,
		"T": 256,
		"O": 4096,
		"I": 8192,
	}
	var attrs uint32
	for _, attribute := range attrList {
		attrs |= fileAttributeMap[strings.ToUpper(attribute)]
	}
	err = syscall.SetFileAttributes(lpFilePath, attrs)
	return err
}

func (s osScenarioHelper) setAttributesForLocalFiles(c asserter, dirPath string, fileList []string, attrList []string) { //nolint:golint,unused
	for _, fileName := range fileList {
		err := s.setAttributesForLocalFile(filepath.Join(dirPath, fileName), attrList)
		c.AssertNoErr(err)
	}
}

func (osScenarioHelper) getFileDates(c asserter, filePath string) (createdTime, lastWriteTime time.Time) {
	h, err := common.GetFileInformation(filePath)
	c.AssertNoErr(err)
	hi := ste.HandleInfo{ByHandleFileInformation: h} // TODO: do we want to rely on AzCopy code in tests like this? It does save a little time in test framework dev
	return hi.FileCreationTime(), hi.FileLastWriteTime()
}

func (osScenarioHelper) getFileAttrs(c asserter, filepath string) *uint32 {
	fileinfo, err := os.Stat(filepath)
	c.AssertNoErr(err)
	stat := fileinfo.Sys().(*syscall.Win32FileAttributeData)

	return &(stat.FileAttributes)
}

func (osScenarioHelper) getFileSDDLString(c asserter, filepath string) *string {
	srcPtr, err := syscall.UTF16PtrFromString(filepath)
	c.AssertNoErr(err)
	// custom open call, because must specify FILE_FLAG_BACKUP_SEMANTICS to make --backup mode work properly (i.e. our use of SeBackupPrivilege)
	fd, err := windows.CreateFile(srcPtr,
		windows.GENERIC_READ, windows.FILE_SHARE_READ, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	c.AssertNoErr(err)

	buf := make([]byte, 512)
	bufLen := uint32(len(buf))
	status := ntdll.CallWithExpandingBuffer(func() ntdll.NtStatus {
		return ntdll.NtQuerySecurityObject(
			ntdll.Handle(fd),
			windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
			(*ntdll.SecurityDescriptor)(unsafe.Pointer(&buf[0])),
			uint32(len(buf)),
			&bufLen)
	}, &buf, &bufLen)

	c.Assert(status, equals(), ntdll.STATUS_SUCCESS)
	sd := (*windows.SECURITY_DESCRIPTOR)(unsafe.Pointer(&buf[0])) // ntdll.SecurityDescriptor is equivalent
	ret := sd.String()

	return &ret
}

func (osScenarioHelper) setFileSDDLString(c asserter, filepath string, sddldata string) {
	sd, err := windows.SecurityDescriptorFromString(sddldata)
	c.AssertNoErr(err, "Failed to parse SDDL supplied")

	o, _, err := sd.Owner()
	c.AssertNoErr(err)

	g, _, err := sd.Group()
	c.AssertNoErr(err)

	d, _, err := sd.DACL()
	c.AssertNoErr(err)

	secInfo := windows.SECURITY_INFORMATION(windows.DACL_SECURITY_INFORMATION | windows.OWNER_SECURITY_INFORMATION | windows.GROUP_SECURITY_INFORMATION)

	if strings.Contains(sddldata, "D:P") {
		secInfo |= windows.PROTECTED_DACL_SECURITY_INFORMATION
	}

	err = windows.SetNamedSecurityInfo(filepath, windows.SE_FILE_OBJECT, secInfo, o, g, d, nil)
	c.AssertNoErr(err)
}

// nolint
func (osScenarioHelper) Mknod(c asserter, path string, mode uint32, dev int) {
	panic("should never be called")
}

// nolint
func (osScenarioHelper) GetUnixStatAdapterForFile(c asserter, filepath string) common.UnixStatAdapter {
	panic("should never be called")
}

func (osScenarioHelper) IsFileHidden(c asserter, filepath string) bool {

	attributes, err := windows.GetFileAttributes(windows.StringToUTF16Ptr(filepath))
	c.AssertNoErr(err)

	// Check if the given file is hidden
	isHidden := attributes&windows.FILE_ATTRIBUTE_HIDDEN != 0
	return isHidden
}
