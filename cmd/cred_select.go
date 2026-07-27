package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/spf13/cobra"
)

func AddSourceDestCredFlags(cmd *cobra.Command, srcCredName, dstCredName *string) {
	cmd.PersistentFlags().StringVar(srcCredName, "src-cred", cred.DefaultNickname,
		"Specify (by nickname) which credential to use for source (see 'azcopy login')")
	cmd.PersistentFlags().StringVar(dstCredName, "dst-cred", cred.DefaultNickname,
		"Specify (by nickname) which credential to use for destination (see 'azcopy login')")
}

func AddTargetCredFlags(cmd *cobra.Command, credName *string, customFlagName ...string) {
	flagName := "cred"
	if len(customFlagName) > 0 {
		flagName = customFlagName[0]
	}
	cmd.PersistentFlags().StringVar(credName, flagName, cred.DefaultNickname,
		"Specify (by nickname) which credential to use (see 'azcopy login')")
}
