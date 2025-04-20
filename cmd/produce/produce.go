package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"strings"
	"sync"
)

var produceCmd = &cobra.Command{
	Use:   "produce <topic> --message <message>",
	Short: "Produce messages to a topic",
	Long:  `Produce messages to a topic`,
	Run: func(command *cobra.Command, args []string) {
		if len(args) == 0 {
			err := command.Help()
			cobra.CheckErr(err)
			return
		}

		boostrapServers, err := cmd.GetCurrentClusterBootstrapServers()
		cobra.CheckErr(err)

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(boostrapServers...),
			kgo.ConsumerGroup(cmd.ConsumerGroup),
		)
		cobra.CheckErr(err)
		adminClient := kadm.NewClient(cl)
		defer cl.Close()
		defer adminClient.Close()

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
				_, err := fmt.Fprint(os.Stderr, "Invalid header format. Expected key=value.\n")
				cobra.CheckErr(err)
				os.Exit(1)
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
	},
}

func init() {
	produceCmd.Flags().StringArray("header", []string{}, "Header to add to the message, example: --header key=value")
	produceCmd.Flags().String("key", "", "Key of the message")
	produceCmd.Flags().String("message", "", "Message to produce")

	err := produceCmd.MarkFlagRequired("message")
	cobra.CheckErr(err)

	cmd.RootCmd.AddCommand(produceCmd)
}
