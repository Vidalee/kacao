package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var getContextsCmd = &cobra.Command{
	Use:   "get-contexts",
	Short: "Display contexts defined in the Kacao configuration",
	Run: func(cmd *cobra.Command, args []string) {
		if !viper.IsSet("contexts") {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), "No contexts defined in the configuration.")
			cobra.CheckErr(err)
			return
		}

		contexts := viper.GetStringMap("contexts")
		if len(contexts) == 0 {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), "No contexts defined in the configuration.")
			cobra.CheckErr(err)
			return
		}

		_, err := fmt.Fprintln(cmd.OutOrStdout(), "Contexts defined in the configuration:")
		cobra.CheckErr(err)

		for contextName, contextConfig := range contexts {
			cluster := contextConfig.(map[string]interface{})["cluster"]
			consumerGroup := contextConfig.(map[string]interface{})["consumer-group"]
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "- %s: consumer-group=%v cluster=%v\n", contextName, consumerGroup, cluster)
			cobra.CheckErr(err)
		}
	},
}

func init() {
	configCmd.AddCommand(getContextsCmd)
}
