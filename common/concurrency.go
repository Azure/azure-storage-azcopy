package common

import (
	"log"
	"os"
	"strconv"
)

// Get the value of environment variable AZCOPY_CONCURRENCY_VALUE
// If the environment variable is set, it defines the number of concurrent connections
// transfer engine will spawn. If not set, transfer engine will spawn the default number
// of concurrent connections
func ComputeConcurrencyValue(numOfCPUs int) int {
	concurrencyValueOverride := os.Getenv("AZCOPY_CONCURRENCY_VALUE")
	if concurrencyValueOverride != "" {
		val, err := strconv.ParseInt(concurrencyValueOverride, 10, 64)
		if err != nil {
			log.Fatalf("error parsing the env AZCOPY_CONCURRENCY_VALUE %q failed with error %v",
				concurrencyValueOverride, err)
		}
		return int(val)
	}

	// fix the concurrency value for smaller machines
	if numOfCPUs <= 4 {
		return 32
	}

	// for machines that are extremely powerful, fix to 300 to avoid running out of file descriptors
	if 16*numOfCPUs > 300 {
		return 300
	}

	// for moderately powerful machines, compute a reasonable number
	return 16 * numOfCPUs
}
