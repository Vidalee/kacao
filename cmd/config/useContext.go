package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var useContextCmd = &cobra.Command{
	Use:   "use-context <name>",
	Short: "Set the current context",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			err := cmd.Help()
			cobra.CheckErr(err)
			return
		}
		contextName := args[0]

		if !viper.IsSet("contexts." + contextName) {
			fmt.Printf("Context '%s' does not exist in the configuration.\n", contextName)
			os.Exit(1)
		}

		viper.Set("current-context", contextName)
		err := viper.WriteConfig()
		cobra.CheckErr(err)
	},
}

func init() {
	configCmd.AddCommand(useContextCmd)
}
