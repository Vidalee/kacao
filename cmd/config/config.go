package config

import (
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage Kacao configuration",
	Long:  `Modify Kacao configuration using subcommands like "kacao config set-cluster"`,
}

func init() {
	cmd.RootCmd.AddCommand(configCmd)
}
