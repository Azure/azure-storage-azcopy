package common

import "regexp"

// PathTraversalNameRegex matches any path segment that is fully comprised of dots
// ("."), e.g. ".." or "./", on either a forward- or backslash-delimited path.
// Such a segment is not representable as a real directory name on most filesystems, and, if encountered in
// a downloaded relative path (e.g. "../foo"), would resolve outside of the destination directory the user
// targeted. Both the front-end (traverser) and the storage engine (ste) rely on this to detect such names.
var PathTraversalNameRegex = regexp.MustCompile("(?i)(^|[/\\\\])\\.+($|[/\\\\])")
