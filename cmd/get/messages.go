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
	"strings"
	"syscall"
)

var messagesCmd = &cobra.Command{
	Use:   "messages <topic_name> [--limit <limit>] [--key key] [--header <key=value>]",
	Short: "Get messages from a topic",
	Long: `Get messages from a topic

This command will retrieve {limit} messages from each partition of the specified topic. Then filter for the {limit} most recent messages.

If you are filtering by header, you may get less than {limit} messages since the filter is applied after the messages are retrieved.
If multiple headers are provided, all must match. Putting * as the value will match any value.

Example:
- kacao get messages <topic_name> --limit 10 --key my-key --header key1=value1 --header key2=*
Will retrieve 10 messages from each partition of the topic <topic_name> and filter for messages that have for key "my-key", and headers with key1=value1 and key2 having any value.
`,
	Args: cobra.ExactArgs(1),
	RunE: func(command *cobra.Command, args []string) error {
		boostrapServers, err := cmd.GetCurrentClusterBootstrapServers()
		cobra.CheckErr(err)
		consumerGroup, err := cmd.GetConsumerGroup()
		cobra.CheckErr(err)

		limit, err := command.Flags().GetInt64("limit")
		cobra.CheckErr(err)
		headers, err := command.Flags().GetStringArray("header")
		cobra.CheckErr(err)
		headersMap := make(map[string]string)
		for _, header := range headers {
			parts := strings.Split(header, "=")
			if len(parts) != 2 {
				return fmt.Errorf("Invalid header format: %s. Expected key=value.\n", header)
			}
			headersMap[parts[0]] = parts[1]
		}
		keyFilter, err := command.Flags().GetString("key")
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

		committedListedOffsets, err := adminClient.ListEndOffsets(ctx, args[0])
		cobra.CheckErr(err)
		for _, listedOffset := range committedListedOffsets[args[0]] {
			if listedOffset.Err != nil {
				return fmt.Errorf("error listing offsets for topic '%s': %v\n", args[0], listedOffset.Err)
			}
		}

		var limitsByPartition = make(map[int32]int64)
		var counterByPartition = make(map[int32]int64)
		for _, listedOffset := range committedListedOffsets[args[0]] {
			limitsByPartition[listedOffset.Partition] = 10
			counterByPartition[listedOffset.Partition] = 0
		}

		var newOffsets kadm.Offsets = make(map[string]map[int32]kadm.Offset)
		newOffsets[args[0]] = make(map[int32]kadm.Offset)
		for _, listedOffset := range committedListedOffsets[args[0]] {
			var offsetValue = listedOffset.Offset - limit

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
			_, err := fmt.Fprintf(command.OutOrStdout(), "Closing client...")
			cl.Close()
			adminClient.Close()
			cobra.CheckErr(err)
			fmt.Println("Client closed")
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
			offSetDiff := a.Offset - b.Offset
			if offSetDiff > 0 {
				return -1
			}
			if offSetDiff < 0 {
				return 1
			}
			return 0
		})

		if len(records) > int(limit) {
			records = records[:limit]
		}

		if len(keyFilter) > 0 {
			var filteredRecords []kgo.Record
			for _, record := range records {
				if string(record.Key) == keyFilter {
					filteredRecords = append(filteredRecords, record)
				}
			}
			records = filteredRecords
		}

		if len(headers) > 0 || len(keyFilter) > 0 {
			var filteredRecords []kgo.Record
			for _, record := range records {
				match := false
				for _, recordHeader := range record.Headers {
					if _, ok := headersMap[recordHeader.Key]; ok {
						if headersMap[recordHeader.Key] != "*" && string(recordHeader.Value) != headersMap[recordHeader.Key] {
							match = false
							break
						} else {
							match = true
						}
					}
				}
				if match {
					filteredRecords = append(filteredRecords, record)
				}
			}
			records = filteredRecords
		}

		_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%-25s%-25s%-25s%-25s%-25s\n", "Topic", "Partition", "Offset", "Key", "Value", "Headers")

		for _, record := range records {
			if len(record.Headers) == 0 {
				_, err := fmt.Fprintf(command.OutOrStdout(), "%-25s%-25d%-25d%-25s%-25s\n",
					record.Topic, record.Partition, record.Offset, string(record.Key), string(record.Value))
				cobra.CheckErr(err)
				continue
			}

			var headers []string
			for _, header := range record.Headers {
				headers = append(headers, fmt.Sprintf("%s: %s", header.Key, string(header.Value)))
			}
			headersString := strings.Join(headers, ", ")

			_, err := fmt.Fprintf(command.OutOrStdout(), "%-25s%-25d%-25d%-25s%-25s%-25s\n",
				record.Topic, record.Partition, record.Offset, string(record.Key), string(record.Value), headersString)
			cobra.CheckErr(err)
		}

		return nil
	},
}

func init() {
	messagesCmd.Flags().Int64P("limit", "l", 10, "Limit the number of messages to get.")
	messagesCmd.Flags().StringP("key", "k", "", "Filter messages by key, example: --key value")
	messagesCmd.Flags().StringArrayP("header", "H", []string{}, "Filter messages by header, example: --header key=value.")

	getCmd.AddCommand(messagesCmd)
}
