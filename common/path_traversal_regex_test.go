package common

import (
	"testing"
)

// Test_PathTraversalNameRegex verifies that PathTraversalNameRegex correctly identifies
// path segments that are made up fully of dots (which cannot be represented as a real
// directory/file name on most filesystems and could resolve outside the download target).
func Test_PathTraversalNameRegex(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		shouldMatch bool
	}{
		{"leading parent traversal", "../foo", true},
		{"parent traversal mid-path", "a/../foo", true},
		{"backslash parent traversal", "a\\..\\foo", true},
		{"current dir traversal", "./foo", true},
		{"multiple dots at start", ".../foo", true},
		{"single dot at start", ".", true},
		{"single dot mid-path", "foo/.", true},
		{"trailing parent traversal", "foo/..", true},
		{"trailing current dir", "foo/.", true},
		{"uppercase traversal", "Foo/../Bar", true},
		{"just two dots with backslash", "..\\", true},

		// Real, fully-qualified Windows paths (including long-path / UNC \\?\ form)
		// as the downloader would produce them for info.Destination.
		{"real win drive normal", `C:\Users\adreed\Documents\xfer\foo.txt`, false},
		{"real win path parent traversal", `C:\Users\adreed\foo\..\bar.txt`, true},
		{"real win path current dir segment", `C:\Users\adreed\Downloads\.\foo.txt`, true},
		{"real UNC normal", `\\?\C:\Users\adreed\Documents\project\foo.txt`, false},
		{"real UNC parent traversal", `\\?\C:\Users\adreed\a\..\b\foo.txt`, true},
		{"real UNC current dir segment", `\\?\C:\Users\adreed\.\foo.txt`, true},
		{"real UNC server share normal", `\\?\UNC\server\share\folder\foo.txt`, false},
		{"real UNC server share traversal", `\\?\UNC\server\share\..\foo.txt`, true},

		{"normal file", "foo/bar.txt", false},
		{"file starting with dot", ".gitignore", false},
		{"file ending with dot inside name", "foo/bar.txt", false},
		{"name containing trailing dot", "foo/bar.", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := PathTraversalNameRegex.MatchString(tt.input)
			if m != tt.shouldMatch {
				t.Errorf("PathTraversalNameRegex.MatchString(%q) = %v, want %v", tt.input, m, tt.shouldMatch)
			}
		})
	}
}
