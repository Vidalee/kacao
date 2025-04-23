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
	Use:   "messages <topic_name> [--limit <limit>]",
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

		var limitsByPartition = make(map[int32]int64)
		var counterByPartition = make(map[int32]int64)
		for _, listedOffset := range committedListedOffsets[args[0]] {
			limitsByPartition[listedOffset.Partition] = 10
			counterByPartition[listedOffset.Partition] = 0
		}

		var newOffsets kadm.Offsets = make(map[string]map[int32]kadm.Offset)
		newOffsets[args[0]] = make(map[int32]kadm.Offset)
		for _, listedOffset := range committedListedOffsets[args[0]] {
			var offsetValue int64 = listedOffset.Offset - limit

			if offsetValue < 0 {
				offsetValue = 0
				limitsByPartition[listedOffset.Partition] = listedOffset.Offset
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

		records := make([]kgo.Record, 0)

		for {
			fetches := cl.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				panic(fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()

			for !iter.Done() {
				record := *iter.Next()
				if counterByPartition[record.Partition] >= limitsByPartition[record.Partition] {
					continue
				}
				counterByPartition[record.Partition]++
				records = append(records, record)
			}

			allCountersReached := true
			for partition, counter := range counterByPartition {
				if counter < limitsByPartition[partition] {
					allCountersReached = false
					break
				}
			}
			if allCountersReached {
				break
			}
		}

		slices.SortFunc(records, func(a, b kgo.Record) int {
			diff := a.Timestamp.Unix() - b.Timestamp.Unix()
			if diff > 0 {
				return -1
			}
			if diff < 0 {
				return 1
			}
			return 0
		})

		if len(records) > int(limit) {
			records = records[:limit]
		}

		for _, record := range records {
			fmt.Printf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n", record.Topic, record.Partition, record.Offset, string(record.Key), string(record.Value))
			for _, header := range record.Headers {
				fmt.Printf("Header: %s: %s\n", header.Key, string(header.Value))
			}
		}
	},
}

func init() {
	messagesCmd.Flags().Int64("limit", 10, "Limit the number of messages to get.")

	getCmd.AddCommand(messagesCmd)
}
