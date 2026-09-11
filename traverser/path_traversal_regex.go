package traverser

import "regexp"

// PathTraversalNameRegex any path segment, fully comprised of .
var PathTraversalNameRegex = regexp.MustCompile("(^|[/\\\\])(\\.|%2e)+($|[/\\\\])")
