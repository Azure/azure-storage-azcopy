package common

import (
	"golang.org/x/exp/constraints"
	"strconv"
)

var MegaSize = []string{
	"B",
	"KB",
	"MB",
	"GB",
	"TB",
	"PB",
	"EB",
}

func ByteSizeToString[T constraints.Integer](size T, megaUnits bool) string {
	units := []string{
		"B",
		"KiB",
		"MiB",
		"GiB",
		"TiB",
		"PiB",
		"EiB", // Let's face it, a file, account, or container probably won't be more than 1000 exabytes in YEARS.
		// (and int64 literally isn't large enough to handle too many exbibytes. 128 bit processors when)
	}
	unit := 0
	floatSize := float64(size)
	gigSize := 1024

	if megaUnits {
		gigSize = 1000
		units = MegaSize
	}

	for floatSize/float64(gigSize) >= 1 {
		unit++
		floatSize /= float64(gigSize)
	}

	return strconv.FormatFloat(floatSize, 'f', 2, 64) + " " + units[unit]
}
