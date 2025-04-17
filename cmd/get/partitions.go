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
	Run: func(command *cobra.Command, args []string) {
		if len(args) == 0 {
			err := command.Help()
			cobra.CheckErr(err)
			return
		}
		if len(args) > 1 {
			fmt.Println("Error: Only one topic name is allowed.")
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
		listedOffsetsByTopic, err := (*kadm.Client).ListCommittedOffsets(adminClient, ctx, args[0])
		cobra.CheckErr(err)
		listedOffsets := listedOffsetsByTopic[args[0]]
		if len(listedOffsets) == 0 {
			fmt.Printf("No partitions found for topic '%s'. Does it exist?\n", args[0])
			return
		}

		orderedListedOffsets := make([]int32, 0, len(listedOffsets))
		for partition := range listedOffsets {
			orderedListedOffsets = append(orderedListedOffsets, partition)
		}
		slices.Sort(orderedListedOffsets)

		fmt.Printf("%-25s%-25s%-25s%-25s%-25s\n", "Topic", "Partition", "Record Offset", "Leader Epoch", "Timestamp")

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

			fmt.Printf("%-25s%-25d%-25d%-25d%-25s\n", args[0], partition,
				listedOffset.Offset, listedOffset.LeaderEpoch, formattedTimestamp)
		}
	},
}

func init() {
	getCmd.AddCommand(partitionsCmd)
}
