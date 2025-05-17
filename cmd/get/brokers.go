package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var brokersCmd = &cobra.Command{
	Use:   "brokers",
	Short: "Display brokers of the current cluster",
	Long:  `Display brokers of the current cluster`,
	RunE: func(command *cobra.Command, args []string) error {
		boostrapServers, err := cmd.GetCurrentClusterBootstrapServers()
		cobra.CheckErr(err)
		consumerGroup, err := cmd.GetConsumerGroup()
		cobra.CheckErr(err)
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(boostrapServers...),
			kgo.ConsumerGroup(consumerGroup),
		)
		cobra.CheckErr(err)
		adminClient := kadm.NewClient(cl)
		defer cl.Close()
		defer adminClient.Close()

		ctx := context.Background()

		brokerDetails, err := (*kadm.Client).ListBrokers(adminClient, ctx)
		cobra.CheckErr(err)

		_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%-20s%-10s%-10s\n", "Broker ID", "Host", "Port", "Rack")
		cobra.CheckErr(err)
		for _, brokerDetail := range brokerDetails {
			_, err := fmt.Fprintf(command.OutOrStdout(), "%-25d%-20s%-10d%-10v\n",
				brokerDetail.NodeID,
				brokerDetail.Host,
				brokerDetail.Port,
				brokerDetail.Rack,
			)
			cobra.CheckErr(err)
		}
		return nil
	},
}

func init() {
	getCmd.AddCommand(brokersCmd)
}
