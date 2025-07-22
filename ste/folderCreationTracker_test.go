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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

// This is mocked to test the folder creation tracker
type mockedJPPH struct {
	folderName []string
	index      []int
	status     []*JobPartPlanTransfer
}

func (jpph *mockedJPPH) CommandString() string                            { panic("Not implemented") }
func (jpph *mockedJPPH) GetRelativeSrcDstStrings(uint32) (string, string) { panic("Not implemented") }
func (jpph *mockedJPPH) JobPartStatus() common.JobStatus                  { panic("Not implemented") }
func (jpph *mockedJPPH) JobStatus() common.JobStatus                      { panic("Not implemented") }
func (jpph *mockedJPPH) SetJobPartStatus(common.JobStatus)                { panic("Not implemented") }
func (jpph *mockedJPPH) SetJobStatus(common.JobStatus)                    { panic("Not implemented") }
func (jpph *mockedJPPH) Transfer(idx uint32) *JobPartPlanTransfer {
	return jpph.status[idx]
}
func (jpph *mockedJPPH) TransferSrcDstRelatives(uint32) (string, string) { panic("Not implemented") }
func (jpph *mockedJPPH) TransferSrcDstStrings(uint32) (string, string, bool) {
	panic("Not implemented")
}
func (jpph *mockedJPPH) TransferSrcPropertiesAndMetadata(uint32) (common.ResourceHTTPHeaders, common.Metadata, blob.BlobType, blob.AccessTier, bool, bool, bool, common.InvalidMetadataHandleOption, common.EntityType, string, string, common.BlobTags) {
	panic("Not implemented")
}

// This test verifies that when we call dir create for a directory, it is created only once,
// even if multiple routines request it to be created.
func TestFolderCreationTracker_directoryCreate(t *testing.T) {
	a := assert.New(t)

	// create a plan with one registered and one unregistered folder
	folderReg := "folderReg"
	folderUnReg := "folderUnReg"

	plan := &mockedJPPH{
		folderName: []string{folderReg, folderUnReg},
		index:      []int{0, 1},
		status: []*JobPartPlanTransfer{
			&JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.NotStarted()},
			&JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.NotStarted()},
		},
	}

	fct := &jpptFolderTracker{
		plan:                   plan,
		mu:                     &sync.Mutex{},
		contents:               make(map[string]uint32),
		unregisteredButCreated: make(map[string]struct{}),
		lockFolderCreation:     true, // we want to test the locking behavior
	}

	// 1. Register folder1
	fct.RegisterPropertiesTransfer(folderReg, 0)

	// Multiple calls to create folderReg should execute create only once.
	numOfCreations := int32(0)
	var wg sync.WaitGroup
	doCreation := func() error {
		atomic.AddInt32(&numOfCreations, 1)
		plan.status[0].atomicTransferStatus = common.ETransferStatus.FolderCreated()
		return nil
	}

	ch := make(chan bool)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			<-ch
			fct.CreateFolder(folderReg, doCreation)
			wg.Done()
		}()
	}
	close(ch) // this will cause all above go rotuines to start creating folder

	wg.Wait()
	a.Equal(int32(1), numOfCreations)

	// similar test for unregistered folder
	numOfCreations = 0
	ch = make(chan bool)
	doCreation = func() error {
		atomic.AddInt32(&numOfCreations, 1)
		plan.status[1].atomicTransferStatus = common.ETransferStatus.FolderCreated()
		return nil
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			<-ch
			fct.CreateFolder(folderUnReg, doCreation)
			wg.Done()
		}()
	}
	close(ch)

	wg.Wait()
	a.Equal(int32(1), numOfCreations)

}
