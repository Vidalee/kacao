package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"slices"
	"time"
)

var partitionsCmd = &cobra.Command{
	Use:   "partitions <topic_name>",
	Short: "Display partitions of a topic",
	Long:  `Display partitions of a topic`,
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
		adminClient := kadm.NewClient(cl)
		defer cl.Close()
		defer adminClient.Close()

		ctx := context.Background()
		listedOffsetsByTopic, err := (*kadm.Client).ListCommittedOffsets(adminClient, ctx, args[0])
		cobra.CheckErr(err)
		listedOffsets := listedOffsetsByTopic[args[0]]

		for _, listedOffset := range listedOffsets {
			if listedOffset.Err != nil {
				return fmt.Errorf("error listing offsets for topic '%s': %v\n", args[0], listedOffset.Err)
			}
		}

		orderedListedOffsets := make([]int32, 0, len(listedOffsets))
		for partition := range listedOffsets {
			orderedListedOffsets = append(orderedListedOffsets, partition)
		}
		slices.Sort(orderedListedOffsets)

		_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%-25s%-25s%-25s%-25s\n", "Topic", "Partition", "Record Offset", "Leader Epoch", "Timestamp")
		cobra.CheckErr(err)

		for _, partition := range orderedListedOffsets {
			listedOffset := listedOffsets[partition]
			cobra.CheckErr(listedOffset.Err)

			var formattedTimestamp string
			if listedOffset.Timestamp == -1 {
				formattedTimestamp = "-1"
			} else {
				timestamp := time.Unix(0, listedOffset.Timestamp*int64(time.Millisecond))
				formattedTimestamp = timestamp.Format(time.RFC3339)
			}

			_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%-25d%-25d%-25d%-25s\n", args[0], partition,
				listedOffset.Offset, listedOffset.LeaderEpoch, formattedTimestamp)
			cobra.CheckErr(err)
		}

		return nil
	},
}

func init() {
	getCmd.AddCommand(partitionsCmd)
}
