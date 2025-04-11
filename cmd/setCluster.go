package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var setClusterCmd = &cobra.Command{
	Use:   "set-cluster NAME --bootstrap-servers localhost:9092",
	Short: "Setup a cluster configuration",
	Long: `Setup a cluster configuration for Kacao.

For local development:
- set-cluster local --bootstrap-servers localhost:9092

For production:
- set-cluster production --bootstrap-servers broker1:9092,broker2:9092,broker3:9092`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			err := cmd.Help()
			if err != nil {
				fmt.Println("Error displaying help:", err)
				return
			}
			return
		}

		clusterName := args[0]
		bootstrapServers, err := cmd.Flags().GetStringSlice("bootstrap-servers")
		cobra.CheckErr(err)

		if len(bootstrapServers) == 0 {
			fmt.Println("Error: --bootstrap-servers flag is required")
			return
		}

		if !isValidClusterName(clusterName) {
			fmt.Println("Error: Cluster name can only contain alphanumerical characters and underscores, and must start with a letter")
			return
		}

		fmt.Printf("Setting up cluster '%s' with bootstrap servers: %s\n", clusterName, bootstrapServers)

		viper.Set("clusters."+clusterName+".bootstrap-servers", bootstrapServers)

		err = viper.WriteConfig()
		if err != nil {
			if os.IsNotExist(err) {
				err := viper.SafeWriteConfig()
				cobra.CheckErr(err)
			} else {
				cobra.CheckErr(err)
			}
		}
	},
}

func init() {
	setClusterCmd.Flags().StringSlice("bootstrap-servers", []string{}, "Comma-separated list of Kafka bootstrap servers")

	configCmd.AddCommand(setClusterCmd)
}

func isValidClusterName(name string) bool {
	if !(name[0] >= 'a' && name[0] <= 'z') {
		return false
	}
	for _, char := range name {
		if !(char >= 'a' && char <= 'z') && char != '_' && !(char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}
