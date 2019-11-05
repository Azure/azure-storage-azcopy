// +build windows

package sddl

import (
	"golang.org/x/sys/windows"
)

func getSIDFromShorthand(shorthand string) (SID, error) {
	wsid, err := windows.StringToSid(shorthand)

	if err != nil {
		return SID{}, err
	}

	// golang.org/x/sys/windows has a SID type of its own. We do not want to use it because this is is a platform portable library.
	return ParseSID(wsid.String())
}
