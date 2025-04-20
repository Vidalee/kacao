package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var getClustersCmd = &cobra.Command{
	Use:   "get-clusters",
	Short: "Display clusters defined in the Kacao configuration",
	Run: func(cmd *cobra.Command, args []string) {
		if !viper.IsSet("clusters") {
			fmt.Println("No clusters defined in the configuration.")
			return
		}

		clusters := viper.GetStringMap("clusters")
		if len(clusters) == 0 {
			fmt.Println("No clusters defined in the configuration.")
			return
		}

		fmt.Println("Clusters defined in the configuration:")
		for clusterName, clusterConfig := range clusters {
			bootstrapServers := clusterConfig.(map[string]interface{})["bootstrap-servers"]
			fmt.Printf("- %s: %v\n", clusterName, bootstrapServers)
		}
	},
}

func init() {
	configCmd.AddCommand(getClustersCmd)
}
