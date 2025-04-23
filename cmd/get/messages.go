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
	"slices"
	"syscall"
)

var messagesCmd = &cobra.Command{
	Use:   "messages",
	Short: "Get messages from a topic",
	Long:  `Get messages from a topic`,
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

		limit, err := command.Flags().GetInt64("limit")
		cobra.CheckErr(err)

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

		var newOffsets kadm.Offsets = make(map[string]map[int32]kadm.Offset)
		newOffsets[args[0]] = make(map[int32]kadm.Offset)
		for _, listedOffset := range committedListedOffsets[args[0]] {
			var offsetValue int64 = 0

			if listedOffset.Offset-limit > 0 {
				offsetValue = listedOffset.Offset - limit
			}

			newOffsets[args[0]][listedOffset.Partition] = kadm.Offset{
				Topic:       listedOffset.Topic,
				Partition:   listedOffset.Partition,
				At:          offsetValue,
				LeaderEpoch: listedOffset.LeaderEpoch,
				Metadata:    "",
			}
		}
		err = adminClient.CommitAllOffsets(ctx, consumerGroup, newOffsets)
		cobra.CheckErr(err)

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigChan
			fmt.Println("Closing client...")
			cl.Close()
			adminClient.Close()
			os.Exit(0)
		}()

		records := make([]kgo.Record, limit)

		for {
			fetches := cl.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				panic(fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()

			counterByPartition := make(map[int32]int64)
			for i, _ := range committedListedOffsets[args[0]] {
				counterByPartition[i] = 0
			}

			for !iter.Done() {
				slices.AppendSeq(records, iter.Next())
			}
		}
	},
}

func init() {
	messagesCmd.Flags().Int64("limit", 10, "Limit the number of messages to get")

	getCmd.AddCommand(messagesCmd)
}
