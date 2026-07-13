package common

import "os"

// DEFAULT_FILE_PERM on Windows retains the historical 0644 for backward
// compatibility since Windows does not use a POSIX umask.
var DEFAULT_FILE_PERM os.FileMode = 0644
