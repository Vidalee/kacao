package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var getContextsCmd = &cobra.Command{
	Use:   "get-contexts",
	Short: "Display contexts defined in the Kacao configuration",
	Run: func(cmd *cobra.Command, args []string) {
		if !viper.IsSet("contexts") {
			cmd.Println("No contexts defined in the configuration.")
			return
		}

		contexts := viper.GetStringMap("contexts")
		if len(contexts) == 0 {
			cmd.Println("No contexts defined in the configuration.")
			return
		}

		cmd.Println("Contexts defined in the configuration:")
		for contextName, contextConfig := range contexts {
			cluster := contextConfig.(map[string]interface{})["cluster"]
			cmd.Printf("- %s: cluster=%s\n", contextName, cluster)
		}
	},
}

func init() {
	configCmd.AddCommand(getContextsCmd)
}
