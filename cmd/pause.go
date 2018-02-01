package cmd

import (
	"github.com/spf13/cobra"
	"github.com/Azure/azure-storage-azcopy/handlers"
	"github.com/Azure/azure-storage-azcopy/common"
	"errors"
)

func init() {
	var commandLineInput common.JobID = ""

	// pauseCmd represents the pause command
	pauseCmd := &cobra.Command{
		Use:        "pause",
		SuggestFor: []string{"pase", "ause", "paue"},
		Short:      "pause pauses the existing job for given JobId",
		Long: `pause pauses the existing job for given JobId`,
		Args: func(cmd *cobra.Command, args []string) error {
			// the pause command requires necessarily to have an argument
			// pause jobId -- pause all the parts of an existing job for given jobId

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command only requires jobId")
			}
			commandLineInput = common.JobID(args[0])
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			handlers.HandlePauseCommand(commandLineInput)
		},
	}
	rootCmd.AddCommand(pauseCmd)
}