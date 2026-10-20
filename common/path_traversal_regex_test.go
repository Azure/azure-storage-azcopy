package common

import (
	"runtime"
	"testing"
)

// pathTraversalNameTestCase describes one input and the result PathTraversalNameRegex
// is expected to produce for it on the platform the test is gated to.
type pathTraversalNameTestCase struct {
	name        string
	input       string
	shouldMatch bool
}

// runPathTraversalNameTests runs each case as a subtest and reports any mismatch.
func runPathTraversalNameTests(t *testing.T, tests []pathTraversalNameTestCase) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := PathTraversalNameRegex.MatchString(tt.input)
			if m != tt.shouldMatch {
				t.Errorf("PathTraversalNameRegex.MatchString(%q) = %v, want %v", tt.input, m, tt.shouldMatch)
			}
		})
	}
}

// Test_PathTraversalNameRegex verifies that PathTraversalNameRegex correctly identifies
// path segments that are made up fully of dots (which cannot be represented as a real
// directory/file name on most filesystems and could resolve outside the download target).
//
// This covers only the inputs whose expected result is the same on every platform, i.e.
// relative paths built from "/" that behave identically wherever they are evaluated.
// The platform-specific behaviour is covered by Test_PathTraversalNameRegex_Windows and
// Test_PathTraversalNameRegex_Unix, which are gated on runtime.GOOS.
func Test_PathTraversalNameRegex(t *testing.T) {
	tests := []pathTraversalNameTestCase{
		{"leading parent traversal", "../foo", true},
		{"parent traversal mid-path", "a/../foo", true},
		{"current dir traversal", "./foo", true},
		{"exactly two dots trailing", "foo/..", true},
		{"exactly two dots mid-path", "a/../foo", true},
		{"single dot at start", ".", true},
		{"single dot mid-path", "foo/./bar", true},
		{"trailing parent traversal", "foo/..", true},
		{"trailing current dir", "foo/.", true},
		{"uppercase traversal", "Foo/../Bar", true},

		{"normal file", "foo/bar.txt", false},
		{"file starting with dot", ".gitignore", false},
		{"file ending with dot inside name", "foo/bar.txt", false},
		{"name containing trailing dot", "foo/bar.", false},
		{"empty string", "", false},
	}

	runPathTraversalNameTests(t, tests)
}

// Test_PathTraversalNameRegex_Windows covers the Windows flavour of PathTraversalNameRegex:
// both "/" and "\\" are path separators there, and any segment consisting solely of dots is
// matched, since Windows strips trailing dots from names and such segments cannot exist
// on disk.
func Test_PathTraversalNameRegex_Windows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("this test only applies to Windows")
	}

	tests := []pathTraversalNameTestCase{
		// Backslash-delimited traversals.
		{"backslash-separated traversal", `a\..\foo`, true},
		{"trailing backslash in name", `..\`, true},
		{"backslashes around dots in name", `\..\`, true},

		// Segments of three or more dots are matched on Windows.
		{"multiple dots at start", ".../foo", true},
		{"multiple dots mid-path", "a/.../foo", true},
		{"multiple dots trailing", "foo/...", true},
		{"three-dot file name", "...", true},

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

		// Forward slashes are path separators on Windows too.
		{"real unix path normal", "/home/adreed/xfer/foo.txt", false},
		{"real unix path parent traversal", "/home/adreed/xfer/../bar.txt", true},
		{"real unix path current dir segment", "/home/adreed/xfer/./foo.txt", true},
	}

	runPathTraversalNameTests(t, tests)
}

// Test_PathTraversalNameRegex_Unix covers the Unix flavour of PathTraversalNameRegex:
// "/" is the only path separator, so a backslash is just a regular character that may
// appear within a segment's name (e.g. a file legally named `\..\`), and only segments
// of one or two dots are matched, since names of three or more dots (e.g. "...") are
// legal file/directory names there.
func Test_PathTraversalNameRegex_Unix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("this test only applies to Unix-like systems")
	}

	tests := []pathTraversalNameTestCase{
		// A backslash is part of the name, not a separator, so none of these traverse.
		{"backslash-separated traversal", `a\..\foo`, false},
		{"trailing backslash in name", `..\`, false},
		{"backslashes around dots in name", `\..\`, false},

		// Segments of three or more dots are legal names on Unix.
		{"multiple dots at start", ".../foo", false},
		{"multiple dots mid-path", "a/.../foo", false},
		{"multiple dots trailing", "foo/...", false},
		{"three-dot file name is legal on unix", "...", false},

		// Real, fully-qualified unix paths, as the downloader would produce them for
		// info.Destination on a non-Windows host.
		{"real unix path normal", "/home/adreed/xfer/foo.txt", false},
		{"real unix path parent traversal", "/home/adreed/xfer/../bar.txt", true},
		{"real unix path current dir segment", "/home/adreed/xfer/./foo.txt", true},
	}

	runPathTraversalNameTests(t, tests)
}
