package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
)

func TestResume_Generic(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync()|eOperation.Resume(), eTestFromTo.AllSync(), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
		debugSkipFiles: []string{
			"/fileb",
			"/filec",
		},
	}, nil, testFiles{
		defaultSize: "1K",

		shouldTransfer: []interface{}{
			folder(""),
			f("filea"),
			f("fileb"),
			f("filec"),
			f("filed"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func GenerateLargeResumeScenario() (debugSkipFiles []string, allFiles []interface{}, err error) {
	debugSkipFiles = make([]string, 0)
	allFiles = make([]interface{}, 201)

	allFiles[0] = folder("")

	for idx := 1; idx < len(allFiles); idx++ {
		uuid, err := uuid.NewUUID()
		if err != nil {
			return nil, nil, err
		}

		fname := uuid.String() + ".txt"

		if idx < 51 {
			debugSkipFiles = append(debugSkipFiles, fname)
		}

		allFiles[idx] = f(fname)
	}

	return
}

func TestResume_LargeGeneric(t *testing.T) {
	toSkip, allFiles, err := GenerateLargeResumeScenario()
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	RunScenarios(t, eOperation.CopyAndSync()|eOperation.Resume(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:      true,
		debugSkipFiles: toSkip,
	}, nil, testFiles{
		defaultSize: "1K",

		shouldTransfer: allFiles,
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestResume_PublicSource_BlobTarget(t *testing.T) {
	RunScenarios(
		t,
		// copy only instead of sync because single file sync is not entirely compatible with the test suite
		// the destination file must exist for sync to happen (so, a copy must happen first)
		// in addition,
		eOperation.Copy()|eOperation.Resume(),
		eTestFromTo.Other(common.EFromTo.BlobBlob(), common.EFromTo.BlobLocal()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:      true,
			debugSkipFiles: []string{";"}, // skip the only file is ;
		},
		nil,
		testFiles{
			defaultSize:  "1K",
			sourcePublic: to.Ptr(container.PublicAccessTypeBlob),
			objectTarget: objectTarget{objectName: "a.txt"},

			shouldTransfer: []interface{}{
				f("a.txt"),
			},
		},
		EAccountType.Standard(), EAccountType.Standard(), "",
	)
}

func TestResume_PublicSource_ContainerTarget(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync()|eOperation.Resume(),
		eTestFromTo.Other(common.EFromTo.BlobBlob(), common.EFromTo.BlobLocal()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:      true,
			debugSkipFiles: []string{"a.txt"}, // skip the only file is ;
		},
		nil,
		testFiles{
			defaultSize:  "1K",
			sourcePublic: to.Ptr(container.PublicAccessTypeContainer),

			shouldTransfer: []interface{}{
				f("a.txt"),
				folder("foo"),
				f("foo/bar.txt"),
			},
		},
		EAccountType.Standard(), EAccountType.Standard(), "",
	)
}
