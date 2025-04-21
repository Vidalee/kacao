package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"os/signal"
	"syscall"
)

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages from a topic",
	Long:  `Consume messages from a topic`,
	Run: func(command *cobra.Command, args []string) {
		if len(args) != 1 {
			err := command.Help()
			cobra.CheckErr(err)
			return
		}

		boostrapServers, err := cmd.GetCurrentClusterBootstrapServers()
		cobra.CheckErr(err)
		consumerGroup, err := cmd.GetConsumerGroup()
		cobra.CheckErr(err)
		offsetArg, err := command.Flags().GetString("offset")
		cobra.CheckErr(err)
		if offsetArg != "" && offsetArg != "earliest" && offsetArg != "latest" {
			fmt.Println("Invalid offset argument. Use 'earliest' or 'latest'. By default, the last committed offset by Kacao will be used.")
			return
		}

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(boostrapServers...),
			kgo.ConsumerGroup(consumerGroup),
			kgo.ConsumeTopics(args[0]),
		)
		cobra.CheckErr(err)
		adminClient := kadm.NewClient(cl)
		defer cl.Close()
		defer adminClient.Close()
		ctx := context.Background()

		committedListedOffsets, _ := adminClient.ListEndOffsets(ctx, args...)

		if offsetArg != "" {
			var newOffsets kadm.Offsets = make(map[string]map[int32]kadm.Offset)
			newOffsets[args[0]] = make(map[int32]kadm.Offset)
			for _, listedOffset := range committedListedOffsets[args[0]] {
				var offsetValue int64 = 0
				if offsetArg == "latest" {
					offsetValue = listedOffset.Offset
				}
				newOffsets[args[0]][listedOffset.Partition] = kadm.Offset{
					Topic:       listedOffset.Topic,
					Partition:   listedOffset.Partition,
					At:          offsetValue,
					LeaderEpoch: listedOffset.LeaderEpoch,
					Metadata:    "",
				}

			}
			err := adminClient.CommitAllOffsets(ctx, consumerGroup, newOffsets)
			cobra.CheckErr(err)
		}

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigChan
			fmt.Println("Closing client...")
			cl.Close()
			adminClient.Close()
			os.Exit(0)
		}()

		for {
			fetches := cl.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				panic(fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				fmt.Println(string(record.Value))
			}
		}
	},
}

func init() {
	consumeCmd.Flags().StringP("offset", "o", "", "Offset to start consuming from (latest, earliest, or by default the last committed offset by Kacao)")
	cmd.RootCmd.AddCommand(consumeCmd)
}
