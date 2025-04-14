package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var deleteContextCmd = &cobra.Command{
	Use:   "delete-context <name>",
	Short: "Delete the specified context from the Kacao configuration",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			err := cmd.Help()
			cobra.CheckErr(err)
		}
		contextName := args[0]

		if !viper.IsSet("contexts." + contextName) {
			cmd.Printf("Context '%s' does not exist in the configuration.\n", contextName)
			os.Exit(1)
		}
		contexts := viper.GetStringMap("contexts")
		delete(contexts, contextName)
		viper.Set("contexts", contexts)
		err := viper.WriteConfig()
		cobra.CheckErr(err)
	},
}

func init() {
	configCmd.AddCommand(deleteContextCmd)
}
