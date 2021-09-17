package e2etest

import (
	"testing"

	"github.com/google/uuid"
)

func TestResume_Generic(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync()|eOperation.Resume(), eTestFromTo.AllSync(), eValidate.Auto(), params{
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
	}, EAccountType.Standard(), "")
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

	RunScenarios(t, eOperation.CopyAndSync()|eOperation.Resume(), eTestFromTo.AllSync(), eValidate.Auto(), params{
		recursive:      true,
		debugSkipFiles: toSkip,
	}, nil, testFiles{
		defaultSize: "1K",

		shouldTransfer: allFiles,
	}, EAccountType.Standard(), "")
}