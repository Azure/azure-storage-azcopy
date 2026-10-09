package common

import (
	"runtime"
	"testing"
)

// Test_PathTraversalNameRegex verifies that PathTraversalNameRegex correctly identifies
// path segments that are made up fully of dots (which cannot be represented as a real
// directory/file name on most filesystems and could resolve outside the download target).
//
// On Unix-like systems, only segments of one or two dots are matched ("." and ".."),
// since names of three or more dots (e.g. "...") are legal file/directory names there.
// On Windows, any segment consisting solely of dots is matched, since Windows strips
// trailing dots from names and such segments cannot exist on disk.
func Test_PathTraversalNameRegex(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		shouldMatch     bool
		shouldMatchUnix bool // expected result on unix-like systems
	}{
		{"leading parent traversal", "../foo", true, true},
		{"parent traversal mid-path", "a/../foo", true, true},
		{"backslash parent traversal", "a\\..\\foo", true, true},
		{"current dir traversal", "./foo", true, true},
		{"multiple dots at start", ".../foo", true, false},
		{"multiple dots mid-path", "a/.../foo", true, false},
		{"multiple dots trailing", "foo/...", true, false},
		{"exactly two dots trailing", "foo/..", true, true},
		{"exactly two dots mid-path", "a/../foo", true, true},
		{"single dot at start", ".", true, true},
		{"single dot mid-path", "foo/./bar", true, true},
		{"trailing parent traversal", "foo/..", true, true},
		{"trailing current dir", "foo/.", true, true},
		{"uppercase traversal", "Foo/../Bar", true, true},
		{"just two dots with backslash", "..\\", true, true},

		// Real, fully-qualified Windows paths (including long-path / UNC \\?\ form)
		// as the downloader would produce them for info.Destination.
		{"real win drive normal", `C:\Users\adreed\Documents\xfer\foo.txt`, false, false},
		{"real win path parent traversal", `C:\Users\adreed\foo\..\bar.txt`, true, true},
		{"real win path current dir segment", `C:\Users\adreed\Downloads\.\foo.txt`, true, true},
		{"real UNC normal", `\\?\C:\Users\adreed\Documents\project\foo.txt`, false, false},
		{"real UNC parent traversal", `\\?\C:\Users\adreed\a\..\b\foo.txt`, true, true},
		{"real UNC current dir segment", `\\?\C:\Users\adreed\.\foo.txt`, true, true},
		{"real UNC server share normal", `\\?\UNC\server\share\folder\foo.txt`, false, false},
		{"real UNC server share traversal", `\\?\UNC\server\share\..\foo.txt`, true, true},

		{"normal file", "foo/bar.txt", false, false},
		{"file starting with dot", ".gitignore", false, false},
		{"file ending with dot inside name", "foo/bar.txt", false, false},
		{"name containing trailing dot", "foo/bar.", false, false},
		{"three-dot file name is legal on unix", "...", true, false},
		{"empty string", "", false, false},
	}

	onWindows := runtime.GOOS == "windows"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := tt.shouldMatch
			if !onWindows {
				want = tt.shouldMatchUnix
			}

			m := PathTraversalNameRegex.MatchString(tt.input)
			if m != want {
				t.Errorf("PathTraversalNameRegex.MatchString(%q) = %v, want %v", tt.input, m, want)
			}
		})
	}
}
