package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var setContextCmd = &cobra.Command{
	Use:   "set-context <name> [--cluster=cluster_name] [--consumer-group=group_name]",
	Short: "Setup a context configuration",
	Long: `Setup a context entry in the Kacao configuration.

Example:
- kacao config set-context local --bootstrap-servers localhost:9092 --consumer-group local-consumer-group`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return cmd.Help()
		}

		contextName := args[0]
		clusterName, err := cmd.Flags().GetString("cluster")
		cobra.CheckErr(err)
		consumerGroup, err := cmd.Flags().GetString("consumer-group")
		cobra.CheckErr(err)

		if len(clusterName) == 0 && len(consumerGroup) == 0 {
			return fmt.Errorf("at least one of --cluster or --consumer-group must be specified")
		}

		if len(clusterName) != 0 {
			clusters := viper.GetStringMap("clusters")
			if clusters[clusterName] == nil {
				return fmt.Errorf("cluster '%s' does not exist in the configuration. Get current clusters using 'kacao config get-clusters\n", clusterName)
			}
			viper.Set("contexts."+contextName+".cluster", clusterName)
		}

		if len(consumerGroup) != 0 {
			viper.Set("contexts."+contextName+".consumer-group", consumerGroup)
		}

		_, err = fmt.Fprintf(cmd.OutOrStdout(), "Defined context '%s'\n", contextName)
		cobra.CheckErr(err)

		return viper.WriteConfig()
	},
}

func init() {
	setContextCmd.Flags().String("cluster", "", "The name of the cluster to set up. Get current clusters using 'kacao config get-clusters'")
	setContextCmd.Flags().String("consumer-group", "", "The name of the consumer group to use. If not set, the default consumer group will be used.")

	configCmd.AddCommand(setContextCmd)
}
