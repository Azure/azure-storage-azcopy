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

package cmd

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/JeffreyRichter/enum/enum"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const locationHelpFormat = "Specified to nudge AzCopy when resource detection may not work (e.g. emulator/azure stack); Required for NFS transfers; optional for SMB. Valid Location are Source words (e.g. Blob, File) that specify the source resource type. All valid Locations are: %s"

var locationHelp = func() string {
	validLocations := ""

	isSafeToOutput := func(loc common.Location) bool {
		switch loc {
		case common.ELocation.Benchmark(),
			common.ELocation.None(),
			common.ELocation.Unknown():
			return false
		default:
			return true
		}
	}

	enum.GetSymbols(reflect.TypeOf(common.ELocation), func(enumSymbolName string, enumSymbolValue interface{}) (stop bool) {
		location := enumSymbolValue.(common.Location)

		if isSafeToOutput(location) {
			validLocations += location.String() + ", "
		}

		return false
	})

	return fmt.Sprintf(locationHelpFormat, strings.TrimSuffix(validLocations, ", "))
}()

var locationHelpText = locationHelp

func ValidateArgumentLocation(src string, userSpecifiedLocation string) (common.Location, error) {
	if userSpecifiedLocation == "" {
		inferredLocation := azcopy.InferArgumentLocation(src)

		// If user didn't explicitly specify Location, use what was inferred (if possible)
		if inferredLocation == common.ELocation.Unknown() {
			return common.ELocation.Unknown(), fmt.Errorf("the inferred location could not be identified, or is currently not supported")
		}
		return inferredLocation, nil
	}

	// User explicitly specified Location, therefore, we should respect what they specified.
	var userLocation common.Location
	err := userLocation.Parse(userSpecifiedLocation)
	if err != nil {
		return common.ELocation.Unknown(), fmt.Errorf("invalid --location value specified: %q. "+locationHelpText, userSpecifiedLocation)

	}

	return userLocation, nil
}
