// +build !windows

package sddl

import (
	"errors"
)

// Note that all usages of TranslateSID gracefully handle the error, rather than throwing the error.
func OSTranslateSID(SID string) (string, error) {
	return SID, errors.New("unsupported on this OS")
}
