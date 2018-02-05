package cmd

import (
	"errors"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/handlers"
	"github.com/spf13/cobra"
)

func init() {
	var commandLineInput common.JobID = ""

	// resumeCmd represents the resume command
	resumeCmd := &cobra.Command{
		Use:        "resume",
		SuggestFor: []string{"resme", "esume", "resue"},
		Short:      "resume resumes the existing job for given JobId",
		Long:       `resume resumes the existing job for given JobId`,
		Args: func(cmd *cobra.Command, args []string) error {
			// the resume command requires necessarily to have an argument
			// resume jobId -- resumes all the parts of an existing job for given jobId

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command only requires jobId")
			}
			commandLineInput = common.JobID(args[0])
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			handlers.HandleResumeCommand(commandLineInput)
		},
	}
	rootCmd.AddCommand(resumeCmd)
}
