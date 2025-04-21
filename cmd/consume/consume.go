package get

import (
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages from a topic",
	Long:  `Consume messages from a topic`,
	Run: func(command *cobra.Command, args []string) {
		if len(args) == 0 {
			err := command.Help()
			cobra.CheckErr(err)
			return
		}

		boostrapServers, err := cmd.GetCurrentClusterBootstrapServers()
		cobra.CheckErr(err)
		consumerGroup, err := cmd.GetConsumerGroup()
		cobra.CheckErr(err)
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(boostrapServers...),
			kgo.ConsumerGroup(consumerGroup),
		)
		cobra.CheckErr(err)
		defer cl.Close()
	},
}

func init() {
	consumeCmd.Flags().StringP("offset", "o", "", "Offset to start consuming from (latest, earliest, or by default the last consumed offset by Kacao)")
	cmd.RootCmd.AddCommand(consumeCmd)
}
