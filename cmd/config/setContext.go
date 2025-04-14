package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var setContextCmd = &cobra.Command{
	Use:   "set-context <name> [--cluster=cluster_name]",
	Short: "Setup a context configuration",
	Long: `Setup a context entry in the Kacao configuration.

Example:
- kacao config set-context local --bootstrap-servers localhost:9092`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			err := cmd.Help()
			cobra.CheckErr(err)
		}

		contextName := args[0]
		clusterName, err := cmd.Flags().GetString("cluster")
		cobra.CheckErr(err)

		if len(clusterName) != 0 {
			clusters := viper.GetStringMap("clusters")
			if clusters[clusterName] == nil {
				fmt.Printf("Error: Cluster '%s' does not exist in the configuration. Get current clusters using 'kacao config get-clusters\n", clusterName)
				os.Exit(1)
			}
			viper.Set("contexts."+contextName+".cluster", clusterName)
		}

		fmt.Printf("Defined context '%s'\n", contextName)

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
	setContextCmd.Flags().String("cluster", "", "The name of the cluster to set up. Get current clusters using 'kacao config get-clusters'")

	configCmd.AddCommand(setContextCmd)
}
