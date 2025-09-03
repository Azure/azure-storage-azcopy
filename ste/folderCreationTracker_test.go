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
		fetchTransfer:          plan.getFetchTransfer(t),
		mu:                     &sync.Mutex{},
		contents:               make(map[string]JpptFolderIndex),
		unregisteredButCreated: make(map[string]struct{}),
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
