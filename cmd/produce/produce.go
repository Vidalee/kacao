package produce

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
	"sync"
)

var produceCmd = &cobra.Command{
	Use:   "produce <topic> --message <message> [--key <key>] [--header <key=value>]",
	Short: "Produce messages to a topic",
	Long:  `Produce messages to a topic`,
	Args:  cobra.ExactArgs(1),
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
		defer cl.Close()

		ctx := context.Background()

		message, err := command.Flags().GetString("message")
		cobra.CheckErr(err)

		key, err := command.Flags().GetString("key")
		cobra.CheckErr(err)

		headers, err := command.Flags().GetStringArray("header")
		cobra.CheckErr(err)

		var kafkaHeaders []kgo.RecordHeader

		for _, header := range headers {
			parts := strings.Split(header, "=")
			if len(parts) != 2 {
				return fmt.Errorf("invalid header format. Expected key=value.\n")
			}
			kafkaHeaders = append(kafkaHeaders, kgo.RecordHeader{Key: parts[0], Value: []byte(parts[1])})
		}
		var wg sync.WaitGroup
		wg.Add(1)
		record := &kgo.Record{Topic: args[0], Value: []byte(message), Key: []byte(key), Headers: kafkaHeaders}
		cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
			defer wg.Done()
			cobra.CheckErr(err)
		})
		wg.Wait()

		return nil
	},
}

func init() {
	produceCmd.Flags().StringArrayP("header", "H", []string{}, "Header to add to the message, example: --header key=value")
	produceCmd.Flags().StringP("key", "k", "", "Key of the message")
	produceCmd.Flags().StringP("message", "m", "", "Message to produce")

	err := produceCmd.MarkFlagRequired("message")
	cobra.CheckErr(err)

	cmd.RootCmd.AddCommand(produceCmd)
}
