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
)

///////////////

// represents a set of source files, including what we expect should happen to them
type testFiles struct {
	size string // how big should the files be. Uses the same K, M, G suffixes as benchmark mode's size-per-file

	// names of files that we expect to be transferred
	shouldTransfer []string

	// names of files that we expect NOT to be found by the enumeration
	shouldIgnore []string

	// names of files that we expect to  fail with error (unlike the other fields, this one is composite object instead of just a filename
	shouldFail []failure

	// names of files that we expect to be skipped due to an overwrite setting
	shouldSkip []string
}

type failure struct {
	filename              string
	partialFailureMessage string
}

////

type params struct {
	recursive    bool
	includePath  string
	includeAfter string
	capMbps      float32
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

// New makes a custom TestFromTo, that is not defined by one of our standard functions such as AllSourcesToOneDest
func (TestFromTo) New(desc string, useAllTos bool, froms []common.Location, tos []common.Location) TestFromTo {
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
	return fmt.Sprintf("%s (%v -> %s %v)", tft.desc, tft.froms, destDesc, tft.tos)
}

func (tft TestFromTo) getValues(op Operation) []common.FromTo {
	result := make([]common.FromTo, 0, 4)

	for _, from := range tft.froms {
		for _, to := range tft.tos {

			// replace File -> Blob with File -> File if configured to do so.
			// So that we can use Blob as a generic "remote" to, but still do File->File in those case where that makes more sense
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
					common.EFromTo.BlobLocal(),
					common.EFromTo.LocalFile(),
					common.EFromTo.FileLocal():
					// do nothing, these are fine
				default:
					continue // not supported for sync
				}
			}

			// this one is valid
			result = append(result, fromTo)
		}
	}

	return result
}

//////

// hookHelper is functions that hooks can call to influence test execution
// NOTE: this interface will have to actively evolve as we discover what we need our hooks to do.
type hookHelper interface {
	GetModifyableParameters() *params
	ReCreateSourceFiles()
}

///////

type hookFunc func(h hookHelper)

// hooks contains functions that are called at various points in the running of the test, so that we can do
// custom behaviour (for those func that are not nil).
// NOTE: the funcs you provide here must be threadsafe, because RunTests works in parallel for all its scenarios
type hooks struct {

	// called after all the setup is done, and before AzCopy is actually invoked
	beforeRunJob hookFunc

	// This one is a bit ugly. It relies on our declarative test running creating the "to ignore" files FIRST then
	// calling this, then making the other files.  The ordering assumption, which is implicit in all respects except the name,
	// is a bit ugly.  TODO: to we have any better ideas?
	betweenCreateFilesToIgnoreAndToTransfer hookFunc
}
