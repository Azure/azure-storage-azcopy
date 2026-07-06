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
		"Credential name to use for source (see 'azcopy login')")
	cmd.PersistentFlags().StringVar(&DestCredentialName, "dst-cred", cred.DefaultNickname,
		"Credential name to use for destination (see 'azcopy login')")
}

func AddTargetCredFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVar(&TargetCredentialName, "cred", cred.DefaultNickname,
		"Credential name to use for target (see 'azcopy login')")
}
