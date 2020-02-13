// +build windows

package sddl

import (
	"golang.org/x/sys/windows"
)

// Note that all usages of OSTranslateSID gracefully handle the error, rather than throwing the error.
func OSTranslateSID(SID string) (string, error) {
	wsid, err := windows.StringToSid(SID)

	if err != nil {
		return "", err
	}

	return wsid.String(), nil
}
