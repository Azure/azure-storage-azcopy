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

package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/JeffreyRichter/enum/enum"
	"reflect"
	"strings"
	"testing"
)

///////////

var sanitizer = common.NewAzCopyLogSanitizer() // while this is "only tests", we may as well follow good SAS redaction practices

////////

type comparison struct {
	equals bool
}

func (c comparison) String() string {
	if c.equals {
		return "equals"
	} else {
		return "notEquals"
	}
}

func equals() comparison {
	return comparison{true}
}

func notEquals() comparison {
	return comparison{false}
}

///////

// simplified assertion interface. Allows us to abstract away the specific test harness we are using
// (in case we change it... again)
type asserter interface {
	Assert(obtained interface{}, comp comparison, expected interface{}, comment ...string)
	AssertNoErr(err error, comment ...string)
	Error(reason string)
	Skip(reason string)
	Failed() bool

	// ScenarioName piggy-backs on this interface, in a context-value-like way (ugly, but it works)
	CompactScenarioName() string
}

type testingAsserter struct {
	t                   *testing.T
	compactScenarioName string
	fullScenarioName    string
}

func (a *testingAsserter) formatComments(comment []string) string {
	expandedComment := strings.Join(comment, ", ")
	if expandedComment != "" {
		expandedComment = "\n    " + expandedComment
	}
	return expandedComment
}

// Assert compares its arguments and marks the current test (or subtest) as failed. Unlike gocheck's Assert method,
// in this implementation execution of the test continues (and so subsequent asserts may give additional information)
func (a *testingAsserter) Assert(obtained interface{}, comp comparison, expected interface{}, comment ...string) {
	// do the comparison (our comparison options are deliberately simple)
	// TODO: if obtained or expected is a pointer, do we want to dereference it before comparing?  Do we even need that in our codebase?
	ok := false
	if comp.equals {
		ok = obtained == expected
	} else {
		ok = obtained != expected
	}
	if ok {
		return
	}

	// record the failure
	a.t.Helper() // exclude this method from the logged callstack
	expandedComment := a.formatComments(comment)
	a.t.Logf("Assertion failed in %s\n    Attempted to assert that: (actual) %v %s (expected) %v%s", a.fullScenarioName, obtained, comp, expected, expandedComment)
	a.t.Fail()
}

// AssertNoErr asserts that err is nil, and calls FailNow for immediate termination of the test if err is not nil.
// This does immediate termination, rather than letting the test continue running like Assert() does, because
// with immediate termination it can be used as a guard clause, before things which might otherwise panic due to invalid inputs.
func (a *testingAsserter) AssertNoErr(err error, comment ...string) {
	if err != nil {
		a.t.Helper() // exclude this method from the logged callstack
		redactedErr := sanitizer.SanitizeLogMessage(err.Error())
		a.t.Logf("Error %s%s", redactedErr, a.formatComments(comment))
		a.t.FailNow()
	}
}

func (a *testingAsserter) Error(reason string) {
	a.t.Error(reason)
}

func (a *testingAsserter) Skip(reason string) {
	a.t.Skip(reason)
}

func (a *testingAsserter) Failed() bool {
	return a.t.Failed()
}

func (a *testingAsserter) CompactScenarioName() string {
	return a.compactScenarioName
}

////

type params struct {
	recursive                 bool
	includePath               string
	includePattern            string
	includeAfter              string
	includeAttributes         string
	excludePath               string
	excludePattern            string
	excludeAttributes         string
	capMbps                   float32
	blockSizeMB               float32
	deleteDestination         common.DeleteDestination
	s2sSourceChangeValidation bool
}

// we expect folder transfers to be allowed (between folder-aware resources) if there are no filters that act at file level
func (p params) allowsFolderTransfers() bool {
	return p.includePattern+p.includeAfter+p.includeAttributes+p.excludePattern+p.excludeAttributes == ""
}

//////////////

var eOperation = Operation(0)

type Operation uint8

func (Operation) Copy() Operation        { return Operation(1) }
func (Operation) Sync() Operation        { return Operation(2) }
func (Operation) CopyAndSync() Operation { return eOperation.Copy() & eOperation.Sync() }

func (o Operation) String() string {
	return enum.StringInt(o, reflect.TypeOf(o))
}

// getValues chops up composite values into their parts
func (o Operation) getValues() []Operation {
	switch o {
	case eOperation.Copy(),
		eOperation.Sync():
		return []Operation{o}
	case eOperation.CopyAndSync():
		return []Operation{eOperation.Copy(), eOperation.Sync()}
	default:
		panic("unexpected operation type")
	}
}

/////////////

var eTestFromTo = TestFromTo{}

// TestFromTo is similar to common/FromTo, except that it can have cases where one value represents many possibilities
type TestFromTo struct {
	desc                   string
	useAllTos              bool
	suppressAutoFileToFile bool // TODO: invert this // if true, we won't automatically replace File -> Blob with File -> File. We do that replacement by default because File -> File is the more common scenario (and, for sync, File -> Blob is not even supported currently).
	froms                  []common.Location
	tos                    []common.Location
	filter                 func(to common.FromTo) bool
}

// AllSourcesToOneDest means use all possible sources, and test each source to one destination (generally Blob is the destination,
// except for sources that don't support Blob, in which case, a download to local is done).
// Use this for tests that are primarily about enumeration of the source (rather than support for a wide range of destinations)
func (TestFromTo) AllSourcesToOneDest() TestFromTo {
	return TestFromTo{
		desc:      "AllSourcesToOneDest",
		useAllTos: false,
		froms:     common.ELocation.AllStandardLocations(),
		tos: []common.Location{
			common.ELocation.Blob(), // auto-replaced with File when source is File
			common.ELocation.Local(),
		},
	}
}

// AllSourcesDownAndS2S means use all possible sources, and for each to both Blob and a download to local (except
// when not applicable. E.g. local source doesn't support download; AzCopy's ADLS Gen doesn't (currently) support S2S.
// This is a good general purpose choice, because it lets you do exercise things fairly comprehensively without
// actually getting into all pairwise combinations
func (TestFromTo) AllSourcesDownAndS2S() TestFromTo {
	return TestFromTo{
		desc:      "AllSourcesDownAndS2S",
		useAllTos: true,
		froms:     common.ELocation.AllStandardLocations(),
		tos: []common.Location{
			common.ELocation.Blob(), // auto-replaced with File when source is File
			common.ELocation.Local(),
		},
	}
}

// AllPairs tests literally all Source/Dest pairings that are supported by AzCopy.
// Use this sparingly, because it runs a lot of cases. Prefer AllSourcesToOneDest or AllSourcesDownAndS2S or similar.
func (TestFromTo) AllPairs() TestFromTo {
	return TestFromTo{
		desc:                   "AllPairs",
		useAllTos:              true,
		suppressAutoFileToFile: true, // not needed for AllPairs
		froms:                  common.ELocation.AllStandardLocations(),
		tos:                    common.ELocation.AllStandardLocations(),
	}
}

// AllS2S represents the subset of AllPairs that are S2S transfers
func (TestFromTo) AllS2S() TestFromTo {
	result := TestFromTo{}.AllPairs()
	result.filter = func(ft common.FromTo) bool {
		return ft.IsS2S()
	}
	return result
}

// Specific is for when you want to list one or more specific from-tos that the test should cover.
// Generally avoid this method, because it does not automatically pick up new pairs as we add new supported
// resource types to AzCopy.
func (TestFromTo) Specific(values ...common.FromTo) TestFromTo {
	result := TestFromTo{}.AllPairs()
	result.filter = func(ft common.FromTo) bool {
		for _, v := range values {
			if ft == v {
				return true
			}
		}
		return false
	}
	return result
}

func NewTestFromTo(desc string, useAllTos bool, froms []common.Location, tos []common.Location) TestFromTo {
	return TestFromTo{
		desc:                   desc,
		useAllTos:              useAllTos,
		suppressAutoFileToFile: true, // turn off this fancy trick for custom ones
		froms:                  froms,
		tos:                    tos,
	}
}

// String gives a basic description. It does not rule out invalid/unsupported combinations.
// That's done by the declarativeRunner later
func (tft TestFromTo) String() string {
	destDesc := "one of"
	if tft.useAllTos {
		destDesc = "all of"
	}
	return fmt.Sprintf("%s (%v -> (%s) %v)", tft.desc, tft.froms, destDesc, tft.tos)
}

func (tft TestFromTo) getValues(op Operation) []common.FromTo {
	result := make([]common.FromTo, 0, 4)

	for _, from := range tft.froms {
		haveEnoughTos := false
		for _, to := range tft.tos {
			if haveEnoughTos {
				continue
			}

			// replace File -> Blob with File -> File if configured to do so.
			// So that we can use Blob as a generic "remote" to, but still do File->File in those case where that makes more sense
			// (Specifically, if testing just one of FileFile and FileBlob, it makes much more sense to do FileFile because its the
			// more common case in real usage.)
			if !tft.suppressAutoFileToFile {
				if from == common.ELocation.File() && to == common.ELocation.Blob() {
					to = common.ELocation.File()
				}
			}

			// parse the combination and see if its valid
			var fromTo common.FromTo
			err := fromTo.Parse(from.String() + to.String())
			if err != nil {
				continue // this pairing wasn't valid
			}

			// if we are doing sync, skip combos that are not currently valid for sync
			if op == eOperation.Sync() {
				switch fromTo {
				case common.EFromTo.BlobBlob(),
					common.EFromTo.FileFile(),
					common.EFromTo.LocalBlob(),
					common.EFromTo.BlobLocal():
					// do nothing, these are fine
				default:
					continue // not supported for sync
				}
			}

			// TODO: remove this temp block
			// temp
			if fromTo.From() == common.ELocation.S3() ||
				fromTo.From() == common.ELocation.BlobFS() || fromTo.To() == common.ELocation.BlobFS() {
				continue // until we impelment the declarativeResoucreManagers
			}

			// check filter
			if tft.filter != nil {
				if !tft.filter(fromTo) {
					continue
				}
			}

			// this one is valid
			result = append(result, fromTo)
			if !tft.useAllTos {
				haveEnoughTos = true // we only need the one we just found
			}
		}
	}

	return result
}

////

var eValidate = Validate(0)

type Validate uint8

// TODO: review this enum

// TransferStates validates "which transfers did we attempt, and what was their outcome?"
func (Validate) TransferStates() Validate { return Validate(0) } // has value 0 because we ALWAYS validate this

// Content validates "was file content preserved"?  TODO: do we really want to compare bytes, or use the MD5 hash mechanism?
func (Validate) Content() Validate { return Validate(1) }

// ContentHeaders validates things like Content-Type, Content-Encoding etc
func (Validate) ContentHeaders() Validate { return Validate(2) }

// Metadata validates the name value pairs of Azure metadata
func (Validate) NameValueMetadata() Validate { return Validate(4) }

// LastWriteTime validates preservation of LastWriteTime (i.e. last time content of the file was written).
// Often referred to as Last Modified Time (LMT) in our codebase, but LMT is somewhat ambiguous as a term because it's not
// clear whether it means the time the content was last changed, or the metadata.
// Last Write Time specifically relates to content, and that's the term we use here.
func (Validate) LastWriteTimeTime() Validate { return Validate(8) }

// CreationTime validates the creation time of the file/folder
func (Validate) CreationTime() Validate { return Validate(16) }

// SMBAttributes validates the attributes such as Archive, ReadOnly etc
func (Validate) SMBAttributes() Validate { return Validate(32) }

// SMBPermissions validates preservation SMB permissions
func (Validate) SMBPermissions() Validate { return Validate(64) }

func (v Validate) String() string {
	return enum.StringInt(v, reflect.TypeOf(v))
}

//////

// hookHelper is functions that hooks can call to influence test execution
// NOTE: this interface will have to actively evolve as we discover what we need our hooks to do.
type hookHelper interface {
	// FromTo returns the fromTo for the scenario
	FromTo() common.FromTo

	Operation() Operation

	// GetModifiableParameters returns a pointer to the AzCopy parameters that will be used in the scenario
	GetModifiableParameters() *params

	// GetTestFiles returns (a copy of) the testFiles object that defines which files will be used in the test
	GetTestFiles() testFiles

	// CreateFiles creates the specified files (overwriting any that are already there of the same name)
	CreateFiles(fs testFiles, atSource bool)
}

///////

type hookFunc func(h hookHelper)

// hooks contains functions that are called at various points in the running of the test, so that we can do
// custom behaviour (for those func that are not nil).
// NOTE: the funcs you provide here must be threadsafe, because RunScenarios works in parallel for all its scenarios
type hooks struct {

	// called after all the setup is done, and before AzCopy is actually invoked
	beforeRunJob hookFunc

	// called after AzCopy has started running, but before it has started its first transfer.  Moment of call may be
	// before, during or after AzCopy's scanning phase.  If this hook is set, AzCopy won't open its first file, to start
	// transferring data, until this function executes.
	beforeOpenFirstFile hookFunc
}
