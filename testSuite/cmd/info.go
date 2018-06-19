package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var infoType string

var defaultInfoType = "AzCopyAppPath"

// initializes the info command, its aliases and description.
func init() {
	infoCmd := &cobra.Command{
		Use:     "info",
		Aliases: []string{"info"},
		Short:   "info gets AzCopy related info.",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return fmt.Errorf("invalid arguments for info command")
			}
			infoType = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if infoType == defaultInfoType {
				fmt.Print(GetAzCopyAppPath())
			}
		},
	}
	rootCmd.AddCommand(infoCmd)

	infoCmd.PersistentFlags().StringVar(&infoType, "infoType", defaultInfoType, "info to get.")
}
