package common

import (
	"regexp"
	"runtime"
)

// PathTraversalNameRegex matches any path segment that is fully comprised of dots
// ("."), e.g. ".." or "./". Such a segment is not representable as a real directory name, and, if encountered in
// a downloaded relative path (e.g. "../foo"), would resolve outside of the destination directory the user
// targeted. The storage engine (ste) relies on this to detect such names when downloading.
//
// Which characters delimit segments is OS-dependent: on Windows, both "/" and "\\" are separators, and a
// backslash can never be part of a file name. Everywhere else, "/" is the only separator, so "\\" is just a
// regular character that may appear within a segment's name (e.g. a file legally named `\..\`), and the
// regex must not treat it as one.
var PathTraversalNameRegex = func() *regexp.Regexp {
	if runtime.GOOS == "windows" {
		return regexp.MustCompile("(^|[/\\\\])\\.+($|[/\\\\])")
	}

	return regexp.MustCompile("(^|/)\\.{1,2}($|/)")
}()
