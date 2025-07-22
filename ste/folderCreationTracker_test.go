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
	"fmt"
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

	// Use the new sync.Map-based implementation
	fct := &jpptFolderTracker{
		plan:                   plan,
		contents:               &sync.Map{}, // Changed to sync.Map
		unregisteredButCreated: &sync.Map{}, // Changed to sync.Map
		folderLocks:            &sync.Map{}, // Added folderLocks
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
			fct.CreateFolder(t.Context(), folderReg, doCreation)
			wg.Done()
		}()
	}
	close(ch) // this will cause all above go routines to start creating folder

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
			fct.CreateFolder(t.Context(), folderUnReg, doCreation)
			wg.Done()
		}()
	}
	close(ch)

	wg.Wait()
	a.Equal(int32(1), numOfCreations)
}

// Additional test to verify concurrent registration and creation
func TestFolderCreationTracker_concurrentRegistrationAndCreation(t *testing.T) {
	a := assert.New(t)

	plan := &mockedJPPH{
		folderName: []string{"folder1", "folder2", "folder3"},
		index:      []int{0, 1, 2},
		status: []*JobPartPlanTransfer{
			&JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.NotStarted()},
			&JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.NotStarted()},
			&JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.NotStarted()},
		},
	}

	fct := &jpptFolderTracker{
		plan:                   plan,
		contents:               &sync.Map{},
		unregisteredButCreated: &sync.Map{},
		folderLocks:            &sync.Map{},
	}

	var wg sync.WaitGroup
	numCreations := int32(0)

	// Test concurrent registration and creation
	for i := 0; i < 3; i++ {
		wg.Add(2) // One for registration, one for creation

		// Registration goroutine
		go func(index int) {
			defer wg.Done()
			folderName := plan.folderName[index]
			fct.RegisterPropertiesTransfer(folderName, uint32(index))
		}(i)

		// Creation goroutine
		go func(index int) {
			defer wg.Done()
			folderName := plan.folderName[index]
			doCreation := func() error {
				atomic.AddInt32(&numCreations, 1)
				plan.status[index].atomicTransferStatus = common.ETransferStatus.FolderCreated()
				return nil
			}
			fct.CreateFolder(t.Context(), folderName, doCreation)
		}(i)
	}

	wg.Wait()

	// Each folder should be created exactly once
	a.Equal(int32(3), numCreations)

	// Verify all folders are marked as created
	for i := 0; i < 3; i++ {
		a.Equal(common.ETransferStatus.FolderCreated(), plan.status[i].TransferStatus())
	}
}

// Test ShouldSetProperties with the new implementation
func TestFolderCreationTracker_shouldSetProperties(t *testing.T) {
	a := assert.New(t)

	plan := &mockedJPPH{
		folderName: []string{"created_folder", "not_created_folder"},
		index:      []int{0, 1},
		status: []*JobPartPlanTransfer{
			&JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.FolderCreated()},
			&JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.NotStarted()},
		},
	}

	fct := &jpptFolderTracker{
		plan:                   plan,
		contents:               &sync.Map{},
		unregisteredButCreated: &sync.Map{},
		folderLocks:            &sync.Map{},
	}

	// Register both folders
	fct.RegisterPropertiesTransfer("created_folder", 0)
	fct.RegisterPropertiesTransfer("not_created_folder", 1)

	// Test with True overwrite option
	a.True(fct.ShouldSetProperties("created_folder", common.EOverwriteOption.True(), nil))
	a.True(fct.ShouldSetProperties("not_created_folder", common.EOverwriteOption.True(), nil))

	// Test with False overwrite option
	a.True(fct.ShouldSetProperties("created_folder", common.EOverwriteOption.False(), nil))
	a.False(fct.ShouldSetProperties("not_created_folder", common.EOverwriteOption.False(), nil))
}

// Test StopTracking with the new implementation
func TestFolderCreationTracker_stopTracking(t *testing.T) {
	a := assert.New(t)

	plan := &mockedJPPH{
		folderName: []string{"folder1"},
		index:      []int{0},
		status: []*JobPartPlanTransfer{
			&JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.NotStarted()},
		},
	}

	fct := &jpptFolderTracker{
		plan:                   plan,
		contents:               &sync.Map{},
		unregisteredButCreated: &sync.Map{},
		folderLocks:            &sync.Map{},
	}

	// Register and create a folder
	fct.RegisterPropertiesTransfer("folder1", 0)

	// Verify folder is tracked
	_, exists := fct.contents.Load("folder1")
	a.True(exists)

	// Stop tracking
	fct.StopTracking("folder1")

	// Verify folder is no longer tracked
	_, exists = fct.contents.Load("folder1")
	a.False(exists)

	// Verify folder lock is cleaned up
	_, exists = fct.folderLocks.Load("folder1")
	a.False(exists)
}

// Benchmark test to compare performance
func BenchmarkFolderCreationTracker_CreateFolder(b *testing.B) {
	plan := &mockedJPPH{
		folderName: make([]string, b.N),
		index:      make([]int, b.N),
		status:     make([]*JobPartPlanTransfer, b.N),
	}

	for i := 0; i < b.N; i++ {
		plan.folderName[i] = fmt.Sprintf("folder_%d", i)
		plan.index[i] = i
		plan.status[i] = &JobPartPlanTransfer{atomicTransferStatus: common.ETransferStatus.NotStarted()}
	}

	fct := &jpptFolderTracker{
		plan:                   plan,
		contents:               &sync.Map{},
		unregisteredButCreated: &sync.Map{},
		folderLocks:            &sync.Map{},
	}

	// Pre-register all folders
	for i := 0; i < b.N; i++ {
		fct.RegisterPropertiesTransfer(plan.folderName[i], uint32(i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			folderName := plan.folderName[i%b.N]
			doCreation := func() error {
				plan.status[i%b.N].atomicTransferStatus = common.ETransferStatus.FolderCreated()
				return nil
			}
			fct.CreateFolder(b.Context(), folderName, doCreation)
			i++
		}
	})
}
