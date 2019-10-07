// +build debug

package cmd

import (
	"github.com/spf13/cobra"
)

func setupDebugCpParams(cpCmd *cobra.Command, raw *rawCopyCmdArgs) {
	cpCmd.PersistentFlags().Uint32Var(&raw.adlsFlushThreshold, "flush-threshold", 7500, "Adjust the number of blocks to flush at once on ADLS gen 2")
	cpCmd.PersistentFlags().BoolVar(&raw.introduceLMTFault, "supply-invalid-lmt", false, "Have SIP hand off an invalid LMT to fail a transfer intentionally. In order to use this flag, please set AZCOPY_DEBUG_MODE to \"on\"")
	cpCmd.PersistentFlags().BoolVar(&raw.introduceMD5Fault, "supply-invalid-md5", false, "Intentionally damage the MD5 taken from the local file to fail a transfer. In order to use this flag, please set AZCOPY_DEBUG_MODE to \"on\"")
	cpCmd.PersistentFlags().BoolVar(&raw.introduceLenFault, "supply-invalid-length", false, "Intentionally provide a false destination length to fail the transfer. In order to use this flag, please set AZCOPY_DEBUG_MODE to \"on\"")
}
