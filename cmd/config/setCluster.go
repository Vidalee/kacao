package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var setClusterCmd = &cobra.Command{
	Use:   "set-cluster <name> --bootstrap-servers localhost:9092",
	Short: "Setup a cluster configuration",
	Long: `Setup a cluster configuration for Kacao.

For local development:
- kacao config set-cluster local --bootstrap-servers localhost:9092

For production:
- kacao config set-cluster production --bootstrap-servers broker1:9092,broker2:9092,broker3:9092`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}

		clusterName := args[0]
		bootstrapServers, err := cmd.Flags().GetStringSlice("bootstrap-servers")
		cobra.CheckErr(err)
		fmt.Printf("flags: %v\n", bootstrapServers)

		if !isValidClusterName(clusterName) {
			return fmt.Errorf("cluster name can only contain alphanumerical characters, hyphens, and underscores, and must start with a letter")
		}

		_, err = fmt.Fprintf(cmd.OutOrStdout(), "Setting up cluster '%s' with bootstrap servers: %v\n", clusterName, bootstrapServers)
		cobra.CheckErr(err)

		viper.Set("clusters."+clusterName+".bootstrap-servers", bootstrapServers)

		err = viper.WriteConfig()
		if err != nil {
			return viper.SafeWriteConfig()
		}
		return nil
	},
}

func init() {
	setClusterCmd.Flags().StringSlice("bootstrap-servers", []string{}, "Comma-separated list of Kafka bootstrap servers")
	err := setClusterCmd.MarkFlagRequired("bootstrap-servers")
	cobra.CheckErr(err)
	configCmd.AddCommand(setClusterCmd)
}

func isValidClusterName(name string) bool {
	if !(name[0] >= 'a' && name[0] <= 'z') {
		return false
	}
	for _, char := range name {
		if !(char >= 'a' && char <= 'z') && char != '_' && char != '-' && !(char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}
