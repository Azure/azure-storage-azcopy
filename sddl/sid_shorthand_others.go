//+build !windows

package sddl

import (
	"errors"
	"runtime"
)

func getSIDFromShorthand(shorthand string) (SID, error) {
	return SID{}, errors.New("cannot obtain portable SID from shorthand on " + runtime.GOOS)
}
