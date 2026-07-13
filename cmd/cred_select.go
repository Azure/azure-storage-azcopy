package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/spf13/cobra"
)

var SourceCredentialName = cred.DefaultNickname
var DestCredentialName = cred.DefaultNickname
var TargetCredentialName = cred.DefaultNickname

func AddSourceDestCredFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&SourceCredentialName, "src-cred", cred.DefaultNickname,
		"Specify (by nickname) which credential to use for source (see 'azcopy login')")
	cmd.PersistentFlags().StringVar(&DestCredentialName, "dst-cred", cred.DefaultNickname,
		"Specify (by nickname) which credential to use for destination (see 'azcopy login')")
}

func AddTargetCredFlags(cmd *cobra.Command, customFlagName ...string) {
	flagName := "cred"
	if len(customFlagName) > 0 {
		flagName = customFlagName[0]
	}

	cmd.PersistentFlags().StringVar(&TargetCredentialName, flagName, cred.DefaultNickname,
		"Specify (by nickname) which credential to use (see 'azcopy login')")
}
