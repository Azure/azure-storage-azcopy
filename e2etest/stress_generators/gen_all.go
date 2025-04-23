package main

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
)

const (
	CommonGenerateAnnouncementIncrement = 10_000
)

var RequireValidTarget cobra.PositionalArgs = func(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return errors.New("expected target service as first positional arg")
	}

	_, err := GetAccountResourceManager(args[0])
	if err != nil {
		return fmt.Errorf("invalid target: %w", err)
	}

	return nil
}

var GenAllCommand = &cobra.Command{
	Use: "all <service-uri>",

	RunE: func(cmd *cobra.Command, args []string) error {
		arm, err := GetAccountResourceManager(args[0])
		if err != nil {
			return fmt.Errorf("failed to get account resource manager: %w", err)
		}

		for k, v := range GeneratorRegistry {
			fmt.Println("Generating " + k)
			a := &DummyAsserter{}
			svc := arm.GetService(a, v.PreferredService())
			if a.CaughtError != nil {
				return fmt.Errorf("failed to get service resource manager: %w", err)
			}

			err := v.Generate(svc)
			if err != nil {
				fmt.Printf("failed generating scenario %s: %v\n", k, err)
			}
		}
		return nil
	},

	Args: RequireValidTarget,
}

func init() {
	RootCmd.AddCommand(GenAllCommand)
}
