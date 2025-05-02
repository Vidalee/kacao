package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var deleteClusterCmd = &cobra.Command{
	Use:   "delete-cluster <name>",
	Short: "Delete the specified cluster from the Kacao configuration",
	Long:  `Delete the specified cluster from the Kacao configuration`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}
		clusterName := args[0]

		if !viper.IsSet("clusters." + clusterName) {
			return fmt.Errorf("cluster '%s' does not exist in the configuration", clusterName)
		}

		clusters := viper.GetStringMap("clusters")
		delete(clusters, clusterName)
		viper.Set("clusters", clusters)
		return viper.WriteConfig()
	},
}

func init() {
	configCmd.AddCommand(deleteClusterCmd)
}
