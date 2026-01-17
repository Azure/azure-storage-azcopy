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
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

type mockedJobPlan struct {
	transfers map[JpptFolderIndex]*JobPartPlanTransfer
}

func (plan *mockedJobPlan) getFetchTransfer(t *testing.T) func(index JpptFolderIndex) *JobPartPlanTransfer {
	return func(index JpptFolderIndex) *JobPartPlanTransfer {
		tx, ok := plan.transfers[index]
		assert.Truef(t, ok, "plan file lookup missed: %v", index)

		return tx
	}
}

// This test verifies that when we call dir create for a directory, it is created only once,
// even if multiple routines request it to be created.
func TestFolderCreationTracker_directoryCreate(t *testing.T) {
	a := assert.New(t)

	// create a plan with one registered and one unregistered folder
	folderReg := "folderReg"
	regIdx := JpptFolderIndex{0, 1}
	folderUnReg := "folderUnReg"
	unregIdx := JpptFolderIndex{1, 1} // cheap validation of job part overlap

	plan := &mockedJobPlan{
		transfers: map[JpptFolderIndex]*JobPartPlanTransfer{
			regIdx:   {atomicTransferStatus: common.ETransferStatus.NotStarted()},
			unregIdx: {atomicTransferStatus: common.ETransferStatus.NotStarted()},
		},
	}

	fct := &jpptFolderTracker{
		fetchTransfer: plan.getFetchTransfer(t),
		mu:            &sync.Mutex{},
		contents:      make(map[string]*JpptFolderTrackerState),
	}

	// 1. Register folder1
	fct.RegisterPropertiesTransfer(folderReg, regIdx.PartNum, regIdx.TransferIndex)

	// Multiple calls to create folderReg should execute create only once.
	numOfCreations := int32(0)
	var wg sync.WaitGroup
	doCreation := func() error {
		atomic.AddInt32(&numOfCreations, 1)
		return nil
	}
	ch := make(chan bool)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			<-ch
			err := fct.CreateFolder(folderUnReg, doCreation)
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	close(ch)

	wg.Wait()
	a.Equal(int32(1), numOfCreations)
	a.Equal(common.ETransferStatus.NotStarted(), plan.transfers[unregIdx].atomicTransferStatus) // validate that no state was written
	a.Equal(common.ETransferStatus.NotStarted(), plan.transfers[regIdx].atomicTransferStatus)   // validate that the overlap bug didn't occur

	// register the new folder, validate state persistence
	fct.RegisterPropertiesTransfer(folderUnReg, unregIdx.PartNum, unregIdx.TransferIndex)
	a.Equal(common.ETransferStatus.FolderCreated(), plan.transfers[unregIdx].atomicTransferStatus)
	a.Equal(common.ETransferStatus.NotStarted(), plan.transfers[regIdx].atomicTransferStatus) // validate that the overlap bug didn't occur

	// now test the prereg folder and validate the write occurs on creation
	numOfCreations = 0
	ch = make(chan bool)
	doCreation = func() error {
		atomic.AddInt32(&numOfCreations, 1)
		return nil
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			<-ch
			err := fct.CreateFolder(folderReg, doCreation)
			assert.NoError(t, err)
			wg.Done()
		}()
	}
	close(ch) // this will cause all above go rotuines to start creating folder

	wg.Wait()
	a.Equal(int32(1), numOfCreations)
	a.Equal(common.ETransferStatus.FolderCreated(), plan.transfers[regIdx].atomicTransferStatus)
}

// This test verifies that we can detect an existing folder, and avoid calling create on it again.
// this helps to manage azure files' complaint of too many directory creation requests.
func TestFolderCreationTracker_directoryExists(t *testing.T) {
	a := assert.New(t)

	// create a plan with one registered and one unregistered folder
	folderExists := "folderExists"
	existsIdx := JpptFolderIndex{0, 1}
	folderCreated := "folderCreated"
	createdIdx := JpptFolderIndex{1, 1} // cheap validation of job part overlap
	folderShouldCreate := "folderShouldCreate"
	shouldCreateIdx := JpptFolderIndex{0, 2}

	plan := &mockedJobPlan{
		transfers: map[JpptFolderIndex]*JobPartPlanTransfer{
			existsIdx:       {atomicTransferStatus: common.ETransferStatus.NotStarted()},
			createdIdx:      {atomicTransferStatus: common.ETransferStatus.NotStarted()},
			shouldCreateIdx: {atomicTransferStatus: common.ETransferStatus.NotStarted()},
		},
	}

	fct := &jpptFolderTracker{
		fetchTransfer: plan.getFetchTransfer(t),
		mu:            &sync.Mutex{},
		contents:      make(map[string]*JpptFolderTrackerState),
	}

	fct.RegisterPropertiesTransfer(folderExists, existsIdx.PartNum, existsIdx.TransferIndex)
	fct.RegisterPropertiesTransfer(folderCreated, createdIdx.PartNum, createdIdx.TransferIndex)
	fct.RegisterPropertiesTransfer(folderShouldCreate, shouldCreateIdx.PartNum, shouldCreateIdx.TransferIndex)

	_ = fct.CreateFolder(folderCreated, func() error {
		return nil
	}) // "create" our folder
	err := fct.CreateFolder(folderExists, func() error {
		return common.FolderCreationErrorAlreadyExists{}
	}) // fail creation on not existing
	a.NoError(err, "already exists should be caught") // ensure we caught that error
	expectedFailureErr := errors.New("this creation should fail")
	err = fct.CreateFolder(folderShouldCreate, func() error {
		return expectedFailureErr
	}) // ensure that a natural failure should return properly
	a.Equal(err, expectedFailureErr)

	// validate folder states
	a.Equal(fct.contents[folderCreated].Status, EJpptFolderTrackerStatus.FolderCreated()) // Our created folder should be marked as such.
	a.Equal(plan.transfers[createdIdx].TransferStatus(), common.ETransferStatus.FolderCreated())
	a.Equal(fct.contents[folderExists].Status, EJpptFolderTrackerStatus.FolderExisted()) // Our existing folder should be marked as such.
	a.Equal(plan.transfers[existsIdx].TransferStatus(), common.ETransferStatus.FolderExisted())
	a.Equal(fct.contents[folderShouldCreate].Status, EJpptFolderTrackerStatus.Unseen()) // no status updates should've occurred on a "naturally" failed create.
	a.Equal(plan.transfers[shouldCreateIdx].TransferStatus(), common.ETransferStatus.NotStarted())

	// validate that re-create doesn't trigger on either
	err = fct.CreateFolder(folderCreated, func() error {
		a.Fail("created folders shouldn't be re-created")
		return errors.New("should return nil")
	})
	a.NoError(err)
	err = fct.CreateFolder(folderExists, func() error {
		a.Fail("existing folders shouldn't be re-created")
		return errors.New("should return nil")
	})
	a.NoError(err)

	// validate we can still create normally for our naturally failed folder
	err = fct.CreateFolder(folderShouldCreate, func() error {
		return nil
	})
}
