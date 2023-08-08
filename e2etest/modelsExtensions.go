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
	"reflect"

	"github.com/JeffreyRichter/enum/enum"
	"github.com/aymanjarrousms/azure-storage-azcopy/v10/common"
)

// An extension to common.ELocation to support smb mount location
var ETestLocation = TestLocation(0)

type TestLocation common.Location

func (TestLocation) SMBMount() TestLocation { return TestLocation(10) }

func (l TestLocation) IsLocal() bool {
	return !l.IsRemote()
}

func (l TestLocation) IsRemote() bool {
	if l.isTestLocation() {
		return false // currently there is no remote type in LocationType
	}

	return common.Location(l).IsRemote()
}

func (l TestLocation) IsFolderAware() bool {
	return l.isTestLocation() || common.Location(l).IsFolderAware()
}

func (l TestLocation) String() string {
	if l.isTestLocation() {
		return enum.StringInt(l, reflect.TypeOf(ETestLocation))
	}

	return common.Location(l).String()
}

func (TestLocation) AllStandardLocations() []common.Location {
	locations := common.ELocation.AllStandardLocations()
	locations = append(locations, common.Location(ETestLocation.SMBMount()))
	return locations
}

func (l TestLocation) isTestLocation() bool {
	return l == ETestLocation.SMBMount()
}

// An extesion to common.FromTO to support smb mount to file
var ETestFromTo = TestFromToEnum(0)

type TestFromToEnum common.FromTo

func (TestFromToEnum) SMBMountFile() TestFromToEnum {
	return TestFromToEnum(common.FromToValue(common.Location(ETestLocation.SMBMount()), common.ELocation.File()))
}

func (ft TestFromToEnum) String() string {
	if ft.isFromToType() {
		return enum.StringInt(ft, reflect.TypeOf(ETestFromTo))
	}

	return common.FromTo(ft).String()
}

func (ft *TestFromToEnum) ParseLocation(s string) error {
	var err error
	var val interface{}
	val, err = enum.ParseInt(reflect.TypeOf(ft), s, true, true)

	if err == nil {
		*ft = val.(TestFromToEnum)
	} else {
		commonFromTo := common.FromTo(*ft)
		err = commonFromTo.Parse(s)

		if err == nil {
			*ft = TestFromToEnum(commonFromTo)
		}
	}

	return err
}

func (ft TestFromToEnum) isFromToType() bool {
	return ft == ETestFromTo.SMBMountFile()
}

func (ft TestFromToEnum) IsUpload() bool {
	if ft.isFromToType() {
		return true
	}

	fromTo := common.FromTo(ft)
	return fromTo.IsUpload()
}

func (ft TestFromToEnum) IsDownload() bool {
	if ft.isFromToType() {
		return false
	}

	fromTo := common.FromTo(ft)
	return fromTo.IsDownload()
}

func (ft TestFromToEnum) IsS2S() bool {
	if ft.isFromToType() {
		return false
	}

	fromTo := common.FromTo(ft)
	return fromTo.IsS2S()
}

// Temporary functions to disable all the not supported scenarios by azCopy fork
// Just for focusing on the supported scenarios - to be covered by the tests
type TestFromToEx TestFromTo

// AllSourcesToOneDest means use all possible sources, and test each source to one destination (generally Blob is the destination,
// except for sources that don't support Blob, in which case, a download to local is done).
// Use this for tests that are primarily about enumeration of the source (rather than support for a wide range of destinations)
func (TestFromToEx) AllSourcesToOneDest() TestFromTo {
	return TestFromTo{
		desc:      "AllSourcesToOneDest",
		useAllTos: false,
		froms: []common.Location{
			common.ELocation.Local(),
			common.Location(ETestLocation.SMBMount()),
		},
		tos: []common.Location{
			common.ELocation.Blob(), // auto-replaced with File when source is File
			common.ELocation.Local(),
		},
	}
}

// AllPairs tests literally all Source/Dest pairings that are supported by AzCopy.
// Use this sparingly, because it runs a lot of cases. Prefer AllSourcesToOneDest or AllSourcesDownAndS2S or similar.
func (TestFromToEx) AllPairs() TestFromTo {
	return TestFromTo{
		desc:                   "AllPairs",
		useAllTos:              true,
		suppressAutoFileToFile: true, // not needed for AllPairs
		froms: []common.Location{
			common.ELocation.Local(),
			common.Location(ETestLocation.SMBMount()),
		},
		tos: []common.Location{
			common.ELocation.Blob(),
			common.ELocation.File(),
		},
	}
}

// AllRemove represents the subset of AllPairs that are remove/delete
func (TestFromToEx) AllRemove() TestFromTo {
	return TestFromTo{
		desc:      "AllRemove",
		useAllTos: true,
		froms:     []common.Location{},
		tos:       []common.Location{},
	}
}

func (TestFromToEx) AllSync() TestFromTo {
	return TestFromTo{
		desc:      "AllSync",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.Local(),
			common.Location(ETestLocation.SMBMount()),
		},
		tos: []common.Location{
			common.ELocation.Blob(),
			common.ELocation.File(),
		},
	}
}

func (TestFromToEx) AllSMB() TestFromTo {
	return TestFromTo{
		desc:                   "ALLSMB",
		useAllTos:              true,
		suppressAutoFileToFile: true, // not needed for AllPairs
		froms: []common.Location{
			common.Location(ETestLocation.SMBMount()),
		},
		tos: []common.Location{
			common.ELocation.File(),
		},
	}
}
