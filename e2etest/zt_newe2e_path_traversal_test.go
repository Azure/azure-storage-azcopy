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

package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&PathTraversalSuite{})
}

type PathTraversalSuite struct{}

// Scenario_CopyContainerDownload_SkipPathTraversal validates that a container -> local download
// skips any blob whose name or relative path contains a path segment fully comprised of dots
// (e.g. "../foo"). A blob named that way would otherwise write a file outside of the directory
// the end user targeted, because "../foo" resolves to the parent of the destination.
func (s *PathTraversalSuite) Scenario_CopyContainerDownload_SkipPathTraversal(svm *ScenarioVariationManager) {
	body := NewRandomObjectContentContainer(SizeFromString("1K"))

	// Source object names use percent-encoded dots (each "." -> "%2e") for the nesting that would
	// otherwise look like a traversal (".." encoded as "%2e%2e"). Only file.txt is scheduled and
	// reloads to the destination root.
	srcObjects := ObjectResourceMappingFlat{
		"file.txt":                  ResourceDefinitionObject{Body: body},
		"%2e%2e/donotcopy.txt":      ResourceDefinitionObject{Body: body},
		"%2e%2e":                    ResourceDefinitionObject{Body: body},
		"asdf/%2e%2e/donotcopy.txt": ResourceDefinitionObject{Body: body},
	}

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{
		Objects: srcObjects,
	})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.OAuth(), EExplicitCredentialType.SASToken()}), svm, CreateAzCopyTargetOptions{}),
				dstContainer,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},

				AsSubdir: pointerTo(false),
			},
		})

	parsedStdout := GetTypeOrAssert[*AzCopyParsedCopySyncRemoveStdout](svm, stdout)
	if !svm.Dryrun() {
		// The encoded names contain no literal dot-only segment, so all four files are scheduled.
		svm.AssertNow("must return stdout", Not{IsNil{}}, parsedStdout)
		svm.Assert("only one file should be scheduled as file transfers", Equal{}, parsedStdout.FinalStatus.FileTransfers, uint32(1))
		svm.Assert("only one transfer should be scheduled", Equal{}, parsedStdout.FinalStatus.TotalTransfers, uint32(1))
	}

	// Sanity check: confirm every source object actually landed where we created it.
	ValidateResource[ContainerResourceManager](svm, srcContainer, ResourceDefinitionContainer{
		Objects: srcObjects,
	}, ValidateResourceOptions{})

	// Validate the files came down to the destination as expected: file.txt at the root, and the
	// donotcopy file nested under the %2e%2e directory.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"file.txt":      ResourceDefinitionObject{Body: body, ObjectShouldExist: pointerTo(true)},
			"donotcopy.txt": ResourceDefinitionObject{ObjectShouldExist: pointerTo(false)},
		},
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})
}
