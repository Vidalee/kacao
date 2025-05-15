package describe

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"strconv"
	"time"
)

var partitionCmd = &cobra.Command{
	Use:   "partition <topic_name> <partition_id>",
	Short: "Describe a topic's partition",
	Long:  `Describe a topic's partition`,
	Args: func(cmd *cobra.Command, args []string) error {
		if err := cobra.ExactArgs(2)(cmd, args); err != nil {
			return err
		}
		if _, err := strconv.ParseInt(args[1], 10, 32); err != nil {
			return fmt.Errorf("<partition_id> must be a number")
		}
		return nil
	},
	RunE: func(command *cobra.Command, args []string) error {
		topicName := args[0]
		partitionID64, err := strconv.ParseInt(args[1], 10, 32)
		cobra.CheckErr(err)
		partitionID := int32(partitionID64)

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
		listedOffsetsByTopic, err := (*kadm.Client).ListCommittedOffsets(adminClient, ctx, topicName)
		cobra.CheckErr(err)
		listedOffsets := listedOffsetsByTopic[args[0]]

		for _, listedOffset := range listedOffsets {
			if listedOffset.Err != nil {
				return fmt.Errorf("error listing offsets for topic '%s': %v\n", args[0], listedOffset.Err)
			}
		}

		partitionListedOffset, ok := listedOffsets[partitionID]
		if !ok {
			return fmt.Errorf("no partition found with ID '%d' for topic '%s'. Does it exist?\n", partitionID, args[0])
		}
		cobra.CheckErr(partitionListedOffset.Err)

		topicDetails, err := (*kadm.Client).ListTopicsWithInternal(adminClient, ctx, topicName)
		cobra.CheckErr(err)
		partitionDetail := topicDetails[args[0]].Partitions[partitionID]
		cobra.CheckErr(partitionDetail.Err)

		var formattedTimestamp string
		if partitionListedOffset.Timestamp == -1 {
			formattedTimestamp = "-1"
		} else {
			timestamp := time.Unix(0, partitionListedOffset.Timestamp*int64(time.Millisecond))
			formattedTimestamp = timestamp.Format(time.RFC3339)
		}

		_, err = fmt.Fprintf(command.OutOrStdout(), "%-30s%s\n", "Topic: ", partitionDetail.Topic)
		cobra.CheckErr(err)
		_, err = fmt.Fprintf(command.OutOrStdout(), "%-30s%d\n", "Partition: ", partitionDetail.Partition)
		cobra.CheckErr(err)
		_, err = fmt.Fprintf(command.OutOrStdout(), "%-30s%d\n", "Latest commited offset: ", partitionListedOffset.Offset)
		cobra.CheckErr(err)
		_, err = fmt.Fprintf(command.OutOrStdout(), "%-30s%s\n", "Latest commited timestamp: ", formattedTimestamp)
		cobra.CheckErr(err)
		_, err = fmt.Fprintf(command.OutOrStdout(), "%-30s%d\n", "Leader: ", partitionDetail.Leader)
		cobra.CheckErr(err)
		_, err = fmt.Fprintf(command.OutOrStdout(), "%-30s%d\n", "Leader epoch: ", partitionDetail.LeaderEpoch)
		cobra.CheckErr(err)
		_, err = fmt.Fprintf(command.OutOrStdout(), "%-30s%v\n", "Replicas: ", partitionDetail.Replicas)
		cobra.CheckErr(err)
		_, err = fmt.Fprintf(command.OutOrStdout(), "%-30s%v\n", "Synced replicas: ", partitionDetail.ISR)
		cobra.CheckErr(err)
		_, err = fmt.Fprintf(command.OutOrStdout(), "%-30s%v\n", "Offline replicas: ", partitionDetail.OfflineReplicas)
		cobra.CheckErr(err)

		return nil
	},
}

func init() {
	describeCmd.AddCommand(partitionCmd)
}
