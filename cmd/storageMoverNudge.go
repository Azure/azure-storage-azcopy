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

package cmd

// -----------------------------------------------------------------------------
//
// Goal (GTM experiment): when a customer runs `azcopy copy` for a migration
// scenario that Azure Storage Mover *also* covers, surface a one-line, dismissible
// note pointing them at Storage Mover's fully-managed migration experience.
//
// The two questions this POC answers:
//   1. Is the chosen source -> target pair covered by Storage Mover?
//        -> answered by storageMoverCoverage[fromTo]   (the FromTo enum azcopy
//           already infers from the user's arguments).
//   2. Are all the flags the customer used exposed in Storage Mover?
//        -> answered by walking cmd.Flags().Visit (only *changed* flags) and
//           checking each against storageMoverCompatibleFlags. If the user relies
//           on any azcopy-only behaviour, we stay silent so we never suggest a
//           tool that can't reproduce their command.
//
// This is intentionally additive and side-effect free: it only ever emits an
// informational line, and only when explicitly enabled via the
// AZCOPY_STORAGE_MOVER_NUDGE=true environment variable, so the experiment can be
// rolled out / rolled back without touching transfer behaviour.
// -----------------------------------------------------------------------------

import (
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// storageMoverLearnLink is the Learn doc comparing AzCopy and Storage Mover.
// TODO(POC): replace with the final, tracked GTM link before any real rollout.
const storageMoverLearnLink = "https://learn.microsoft.com/azure/storage-mover/" // #nudge-link

// storageMoverCoverage maps azcopy source->target combinations onto the migration
// paths Azure Storage Mover supports today. Keep this list conservative: only
// pairs Storage Mover can genuinely complete should be present, otherwise the
// nudge becomes noise.
//
// Storage Mover supported paths (mapped to azcopy FromTo):
//   - on-prem/agent source -> Azure Blob container  => Local* and S3 sources to Blob
//   - on-prem/agent source -> Azure Files (SMB)      => Local source to File
//   - on-prem/agent source -> Azure Files (NFS)      => Local source to FileNFS
//
// NOTE / known POC limitation: azcopy represents any local filesystem (including
// a mounted SMB or NFS share) as ELocation.Local(), so it cannot by itself tell
// an SMB mount from an NFS mount the way the Storage Mover agent can. For the POC
// we treat local-source uploads to Azure storage as "potentially covered".
var storageMoverCoverage = map[common.FromTo]bool{
	common.EFromTo.LocalBlob():    true, // upload to Blob container
	common.EFromTo.LocalFile():    true, // upload to Azure Files (SMB)
	common.EFromTo.LocalFileNFS(): true, // upload to Azure Files (NFS)
	common.EFromTo.S3Blob():       true, // Amazon S3 -> Blob container
}

// storageMoverCompatibleFlags is the set of azcopy flags whose intent Storage
// Mover can reproduce (or which are migration-neutral, e.g. output/logging knobs
// that don't change *what* gets moved). If the user changed any flag NOT in this
// set, we assume their command depends on azcopy-specific behaviour that Storage
// Mover does not expose, and we suppress the nudge.
//
// This deliberately errs on the side of silence: it is far better to miss a
// genuine opportunity than to suggest Storage Mover for a command it cannot honour.
var storageMoverCompatibleFlags = map[string]bool{
	// Core migration semantics Storage Mover supports.
	"recursive":                true, // Storage Mover always migrates the subtree
	"from-to":                  true, // just disambiguates the pair we already vetted
	"overwrite":                true, // Storage Mover has mirror/merge overwrite modes
	"put-md5":                  true, // integrity validation
	"check-md5":                true,
	"preserve-info":            true, // metadata/timestamp preservation
	"preserve-smb-info":        true,
	"preserve-permissions":     true, // ACL preservation
	"preserve-smb-permissions": true,

	// Migration-neutral knobs: they affect logging / presentation / throughput,
	// not which files land at the destination, so they don't disqualify a nudge.
	"output-type":  true,
	"output-level": true,
	"log-level":    true,
	"cap-mbps":     true,
	"dry-run":      true,
}

func maybeSuggestStorageMover(cmd *cobra.Command, fromTo common.FromTo) {
	if !storageMoverNudgeEnabled() {
		return
	}

	eligible, reason := evaluateStorageMoverOpportunity(cmd, fromTo)
	if !eligible {
		// Surface the reason only in the (verbose) log so we can measure the
		// funnel during the POC without spamming the user's console.
		common.GetLifecycleMgr().Info("[storage-mover-nudge] not shown: " + reason)
		return
	}

	common.GetLifecycleMgr().Info(storageMoverNudgeMessage())
}

func evaluateStorageMoverOpportunity(cmd *cobra.Command, fromTo common.FromTo) (eligible bool, reason string) {
	if !storageMoverCoverage[fromTo] {
		return false, "pair-not-covered:" + fromTo.String()
	}

	if incompatible := incompatibleFlagsUsed(cmd); len(incompatible) > 0 {
		return false, "incompatible-flags:" + strings.Join(incompatible, ",")
	}

	return true, ""
}

func incompatibleFlagsUsed(cmd *cobra.Command) []string {
	var incompatible []string
	cmd.Flags().Visit(func(f *pflag.Flag) {
		if !storageMoverCompatibleFlags[f.Name] {
			incompatible = append(incompatible, f.Name)
		}
	})
	return incompatible
}

func storageMoverNudgeEnabled() bool {
	v := strings.TrimSpace(common.GetEnvironmentVariable(common.EEnvironmentVariable.StorageMoverNudge()))
	return strings.EqualFold(v, "true") || v == "1"
}

func storageMoverNudgeMessage() string {
	return "Tip: You can now use Azure Storage Mover for a fully managed migration " +
		"experience to move this data. Learn more: " + storageMoverLearnLink
}
