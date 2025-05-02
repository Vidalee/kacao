package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var deleteContextCmd = &cobra.Command{
	Use:   "delete-context <name>",
	Short: "Delete the specified context from the Kacao configuration",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}
		contextName := args[0]

		if !viper.IsSet("contexts." + contextName) {
			return fmt.Errorf("context '%s' does not exist in the configuration", contextName)
		}

		contexts := viper.GetStringMap("contexts")
		delete(contexts, contextName)
		viper.Set("contexts", contexts)
		return viper.WriteConfig()
	},
}

func init() {
	configCmd.AddCommand(deleteContextCmd)
}
