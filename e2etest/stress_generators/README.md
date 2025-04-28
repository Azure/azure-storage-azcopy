# Synthetic stress test generators

A base command, `all <service-url>` is provided to generate all cases on a target telemetry storage account.

In addition, all generators are added as subcommands automatically. Review the `Generator` interface and register your generator in an `init()` command to implement a new one. Utilize the GenerationJobManager to assist with managing threading for your workload.

Review `config.go` to adjust configuration-- it is implemented with the same configuration pioneered in the new E2E test framework, for familiarity.
