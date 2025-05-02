package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var useContextCmd = &cobra.Command{
	Use:   "use-context <name>",
	Short: "Set the current context",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}
		contextName := args[0]

		if !viper.IsSet("contexts." + contextName) {
			return fmt.Errorf("context '%s' does not exist in the configuration", contextName)
		}

		viper.Set("current-context", contextName)
		return viper.WriteConfig()
	},
}

func init() {
	configCmd.AddCommand(useContextCmd)
}
