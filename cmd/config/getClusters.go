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
			_, err := fmt.Fprintln(cmd.OutOrStdout(), "No clusters defined in the configuration.")
			cobra.CheckErr(err)
			return
		}

		clusters := viper.GetStringMap("clusters")
		if len(clusters) == 0 {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), "No clusters defined in the configuration.")
			cobra.CheckErr(err)
			return
		}

		_, err := fmt.Fprintln(cmd.OutOrStdout(), "Clusters defined in the configuration:")
		cobra.CheckErr(err)

		for clusterName, clusterConfig := range clusters {
			bootstrapServers := clusterConfig.(map[string]interface{})["bootstrap-servers"]
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "- %s: %v\n", clusterName, bootstrapServers)
			cobra.CheckErr(err)
		}
	},
}

func init() {
	configCmd.AddCommand(getClustersCmd)
}
