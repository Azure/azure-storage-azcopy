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

package ste

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Global timing statistics for folders
var (
	folderTimingStats = struct {
		mu        sync.Mutex
		totalTime time.Duration
		count     int64
		avgTime   time.Duration

		// Mean tracking for individual steps
		prepMean         time.Duration
		ensureFolderMean time.Duration
		setPropsMean     time.Duration
	}{}
)

// anyToRemote_folder handles all kinds of sender operations for FOLDERS - both uploads from local files, and S2S copies
func anyToRemote_folder(jptm IJobPartTransferMgr, info *TransferInfo, pacer pacer, senderFactory senderFactory, sipf sourceInfoProviderFactory) {
	startTime := time.Now()
	folderName := info.Source
	var timings []string

	// Log all timings in one line when function exits
	defer func() {
		totalTime := time.Since(startTime)

		// Update global timing statistics
		folderTimingStats.mu.Lock()
		folderTimingStats.totalTime += totalTime
		folderTimingStats.count++
		folderTimingStats.avgTime = time.Duration(int64(folderTimingStats.totalTime) / folderTimingStats.count)
		currentAvg := folderTimingStats.avgTime
		currentCount := folderTimingStats.count

		// Parse individual step timings and update means
		if len(timings) >= 3 {
			// Parse timing strings to extract durations
			for i, timing := range timings {
				parts := strings.Split(timing, ":")
				if len(parts) == 2 {
					if duration, err := time.ParseDuration(parts[1]); err == nil {
						switch i {
						case 0: // Prep
							folderTimingStats.prepMean = time.Duration((int64(folderTimingStats.prepMean)*(currentCount-1) + int64(duration)) / currentCount)
						case 1: // EnsureFolder
							folderTimingStats.ensureFolderMean = time.Duration((int64(folderTimingStats.ensureFolderMean)*(currentCount-1) + int64(duration)) / currentCount)
						case 2: // SetProps
							folderTimingStats.setPropsMean = time.Duration((int64(folderTimingStats.setPropsMean)*(currentCount-1) + int64(duration)) / currentCount)
						}
					}
				}
			}
		}

		// Log mean timings every 100,000 transfers
		if currentCount%100000 == 0 {
			meanLine := fmt.Sprintf("FOLDER_TIMING_MEAN: [Count:%d | Prep:%s | EnsureFolder:%s | SetProps:%s | Total:%s]",
				currentCount,
				folderTimingStats.prepMean.String(),
				folderTimingStats.ensureFolderMean.String(),
				folderTimingStats.setPropsMean.String(),
				currentAvg.String())
			jptm.Log(common.LogError, meanLine)
		}

		folderTimingStats.mu.Unlock()

		timingLine := "FOLDER_TIMING: " + folderName + " [" + strings.Join(timings, " | ") + " | Total:" + totalTime.String() + " | Count:" + fmt.Sprintf("%d", currentCount) + "]"
		jptm.Log(common.LogError, timingLine)
	}()

	// step 1. prepare folder transfer (init checks, create srcinfo, create sender)
	step1Start := time.Now()
	if jptm.WasCanceled() {
		/* This is earliest we detect that jptm has been cancelled before we reach destination */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}

	// Create source info provider
	srcInfoProvider, err := sipf(jptm)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	if srcInfoProvider.EntityType() != common.EEntityType.Folder() {
		panic("configuration error. Source Info Provider does not have Folder entity type")
	}

	// Create sender
	baseSender, err := senderFactory(jptm, info.Destination, pacer, srcInfoProvider)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	s, ok := baseSender.(folderSender)
	if !ok {
		jptm.LogSendError(info.Source, info.Destination, "sender implementation does not support folders", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	timings = append(timings, "Prep:"+time.Since(step1Start).String())

	// step 2. ensure folder exists
	// No chunks to schedule. Just run the folder handling operations.
	// There are no checks for folders on LMT's changing while we read them. We need that for files,
	// so we don't use and out-dated size to plan chunks, or read a mix of old and new data, but neither
	// of those issues apply to folders.
	step2Start := time.Now()
	err = s.EnsureFolderExists() // we may create it here, or possible there's already a file transfer for the folder that has created it, or maybe it already existed before this job
	if err != nil {
		switch err {
		case folderPropertiesSetInCreation{}:
			// Continue to standard completion.
		case folderPropertiesNotOverwroteInCreation{}:
			jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "Folder already exists, so due to the --overwrite option, its properties won't be set")
			jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists()) // using same status for both files and folders, for simplicity
			jptm.ReportTransferDone()
			return
		default:
			jptm.FailActiveSend("ensuring destination folder exists", err)
		}
	}
	timings = append(timings, "EnsureFolder:"+time.Since(step2Start).String())

	// step 3. set folder properties
	step3aStart := time.Now()
	if err == nil {
		t := jptm.GetFolderCreationTracker()
		defer t.StopTracking(s.DirUrlToString()) // don't need it after this routine
		shouldSetProps := t.ShouldSetProperties(s.DirUrlToString(), jptm.GetOverwriteOption(), jptm.GetOverwritePrompter())
		if !shouldSetProps {
			jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "Folder already exists, so due to the --overwrite option, its properties won't be set")
			jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists()) // using same status for both files and folders, for simplicity
			jptm.ReportTransferDone()
			return
		}

		err = s.SetFolderProperties()
		if err != nil {
			jptm.FailActiveSend("setting folder properties", err)
		}
	}
	timings = append(timings, "SetProps:"+time.Since(step3aStart).String())

	commonSenderCompletion(jptm, baseSender, info) // for consistency, always run the standard epilogue
}
