package config

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var deleteClusterCmd = &cobra.Command{
	Use:   "delete-cluster <name>",
	Short: "Delete the specified cluster from the Kacao configuration",
	Long:  `Delete the specified cluster from the Kacao configuration`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			err := cmd.Help()
			cobra.CheckErr(err)
			return
		}
		clusterName := args[0]

		if !viper.IsSet("clusters." + clusterName) {
			cmd.Printf("Cluster '%s' does not exist in the configuration.\n", clusterName)
			os.Exit(1)
		}
		clusters := viper.GetStringMap("clusters")
		delete(clusters, clusterName)
		viper.Set("clusters", clusters)
		err := viper.WriteConfig()
		cobra.CheckErr(err)
	},
}

func init() {
	configCmd.AddCommand(deleteClusterCmd)
}
