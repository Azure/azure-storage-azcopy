package main

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type Generator interface {
	Name() string
	Generate(manager e2etest.ServiceResourceManager) error
	RegisterFlags(flags *pflag.FlagSet)
	PreferredService() common.Location
}

var GeneratorRegistry = make(map[string]Generator)

func RegisterGenerator(g Generator) {
	_, ok := GeneratorRegistry[g.Name()]
	if ok {
		panic("Generator " + g.Name() + " re-registered")
	}

	GeneratorRegistry[g.Name()] = g
	cmd := &cobra.Command{
		Use: fmt.Sprintf("%s <service-uri>", g.Name()),
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunGenerator(g.Name(), args[0])
		},
		Args: RequireValidTarget,
	}

	g.RegisterFlags(cmd.PersistentFlags())

	RootCmd.AddCommand(cmd)
}

func RunGenerator(name, target string) error {
	acct, err := GetAccountResourceManager(target)
	if err != nil {
		return fmt.Errorf("failed to get account resource manager: %w", err)
	}

	gen, ok := GeneratorRegistry[name]
	if !ok {
		return fmt.Errorf("generator %s does not exist", name)
	}

	a := &DummyAsserter{}
	svc := acct.GetService(a, gen.PreferredService())

	fmt.Printf("Generating scenario %s... This may take a long time, please be patient.\n", name)
	err = gen.Generate(svc)
	if err != nil {
		return fmt.Errorf("failed generating scenario %s: %w", name, err)
	}

	return nil
}
