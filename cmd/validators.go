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
	"net/url"
	"reflect"
	"regexp"
	"strings"

	"github.com/JeffreyRichter/enum/enum"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func ValidateFromTo(src, dst string, userSpecifiedFromTo string) (common.FromTo, error) {
	if userSpecifiedFromTo == "" {
		inferredFromTo := inferFromTo(src, dst)

		// If user didn't explicitly specify FromTo, use what was inferred (if possible)
		if inferredFromTo == common.EFromTo.Unknown() {
			return common.EFromTo.Unknown(), fmt.Errorf("the inferred source/destination combination could not be identified, or is currently not supported")
		}
		return inferredFromTo, nil
	}

	// User explicitly specified FromTo, therefore, we should respect what they specified.
	var userFromTo common.FromTo
	err := userFromTo.Parse(userSpecifiedFromTo)
	if err != nil {
		return common.EFromTo.Unknown(), fmt.Errorf("invalid --from-to value specified: %q. "+fromToHelpText, userSpecifiedFromTo)

	}

	if userFromTo == common.EFromTo.LocalFileSMB() {
		userFromTo = common.EFromTo.LocalFile()
	} else if userFromTo == common.EFromTo.FileSMBLocal() {
		userFromTo = common.EFromTo.FileLocal()
	} else if userFromTo == common.EFromTo.FileSMBFileSMB() {
		userFromTo = common.EFromTo.FileFile()
	}

	if userFromTo == common.EFromTo.FileSMBFileNFS() || userFromTo == common.EFromTo.FileNFSFileSMB() {
		glcm.Error("The --from-to value of " + userFromTo.String() +
			" is not supported currently. " +
			"Copy operations between SMB and NFS file shares are not supported yet.")
	}

	return userFromTo, nil
}

const fromToHelpFormat = "Specified to nudge AzCopy when resource detection may not work (e.g. piping/emulator/azure stack); Valid FromTo are pairs of Source-Destination words (e.g. BlobLocal, BlobBlob) that specify the source and destination resource types. All valid FromTos are: %s"

var fromToHelp = func() string {
	validFromTos := ""

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

	enum.GetSymbols(reflect.TypeOf(common.EFromTo), func(enumSymbolName string, enumSymbolValue interface{}) (stop bool) {
		fromTo := enumSymbolValue.(common.FromTo)

		if isSafeToOutput(fromTo.From()) && isSafeToOutput(fromTo.To()) {
			fromtoStr := fromTo.String()
			if fromTo.String() == common.EFromTo.LocalFile().String() {
				fromtoStr = "LocalFileSMB"
			} else if fromTo.String() == common.EFromTo.FileLocal().String() {
				fromtoStr = "FileSMBLocal"
			} else if fromTo.String() == common.EFromTo.FileFile().String() {
				fromtoStr = "FileSMBFileSMB"
			}
			validFromTos += fromtoStr + ", "
		}
		return false
	})

	return fmt.Sprintf(fromToHelpFormat, strings.TrimSuffix(validFromTos, ", "))
}()

var fromToHelpText = fromToHelp

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

func inferFromTo(src, dst string) common.FromTo {
	// Try to infer the 1st argument
	srcLocation := InferArgumentLocation(src)
	if srcLocation == srcLocation.Unknown() {
		glcm.Info("Cannot infer source location of " +
			common.URLStringExtension(src).RedactSecretQueryParamForLogging() +
			". Please specify the --from-to switch. " + fromToHelpText)
		return common.EFromTo.Unknown()
	}

	dstLocation := InferArgumentLocation(dst)
	if dstLocation == dstLocation.Unknown() {
		glcm.Info("Cannot infer destination location of " +
			common.URLStringExtension(dst).RedactSecretQueryParamForLogging() +
			". Please specify the --from-to switch. " + fromToHelpText)
		return common.EFromTo.Unknown()
	}

	out := common.EFromTo.Unknown() // Check that the intended FromTo is in the list of valid FromTos; if it's not, return Unknown as usual and warn the user.
	intent := (common.FromTo(srcLocation) << 8) | common.FromTo(dstLocation)
	enum.GetSymbols(reflect.TypeOf(common.EFromTo), func(enumSymbolName string, enumSymbolValue interface{}) (stop bool) { // find if our fromto is a valid option
		fromTo := enumSymbolValue.(common.FromTo)
		// none/unknown will never appear as valid outputs of the above functions
		// If it's our intended fromto, we're good.
		if fromTo == intent {
			out = intent
			return true
		}

		return false
	})

	if out != common.EFromTo.Unknown() {
		return out
	}

	glcm.Info("The parameters you supplied were " +
		"Source: '" + common.URLStringExtension(src).RedactSecretQueryParamForLogging() + "' of type " + srcLocation.String() +
		", and Destination: '" + common.URLStringExtension(dst).RedactSecretQueryParamForLogging() + "' of type " + dstLocation.String())
	glcm.Info("Based on the parameters supplied, a valid source-destination combination could not " +
		"automatically be found. Please check the parameters you supplied.  If they are correct, please " +
		"specify an exact source and destination type using the --from-to switch. " + fromToHelpText)

	return out
}

var IPv4Regex = regexp.MustCompile(`\d+\.\d+\.\d+\.\d+`) // simple regex

func ValidateArgumentLocation(src string, userSpecifiedLocation string) (common.Location, error) {
	if userSpecifiedLocation == "" {
		inferredLocation := InferArgumentLocation(src)

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

func InferArgumentLocation(arg string) common.Location {
	if arg == pipeLocation {
		return common.ELocation.Pipe()
	}
	if startsWith(arg, "http") {
		// Let's try to parse the argument as a URL
		u, err := url.Parse(arg)
		// NOTE: sometimes, a local path can also be parsed as a url. To avoid thinking it's a URL, check Scheme, Host, and Path
		if err == nil && u.Scheme != "" && u.Host != "" {
			// Is the argument a URL to blob storage?
			switch host := strings.ToLower(u.Host); true {
			// Azure Stack does not have the core.windows.net
			case strings.Contains(host, ".blob"):
				return common.ELocation.Blob()
			case strings.Contains(host, ".file"):
				return common.ELocation.FileSMB()
			case strings.Contains(host, ".dfs"):
				return common.ELocation.BlobFS()
			case strings.Contains(host, benchmarkSourceHost):
				return common.ELocation.Benchmark()
				// enable targeting an emulator/stack
			case IPv4Regex.MatchString(host):
				return common.ELocation.Unknown()
			}

			if common.IsS3URL(*u) {
				return common.ELocation.S3()
			}

			if common.IsGCPURL(*u) {
				return common.ELocation.GCP()
			}

			// If none of the above conditions match, return Unknown
			return common.ELocation.Unknown()
		}
	}

	return common.ELocation.Local()
}
