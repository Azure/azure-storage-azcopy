package cmd

import "strings"

// validateString compares the two strings.
func validateString(expected string, actual string) bool {
	if strings.Compare(expected, actual) != 0 {
		return false
	}
	return true
}
