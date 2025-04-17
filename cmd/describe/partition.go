package get

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
		//check if partition id is a number
		if _, err := strconv.ParseInt(args[1], 10, 32); err != nil {
			return fmt.Errorf("<partition_id> must be a number")
		}
		return nil
	},
	Run: func(command *cobra.Command, args []string) {
		topicName := args[0]
		partitionID64, err := strconv.ParseInt(args[1], 10, 32)
		cobra.CheckErr(err)
		partitionID := int32(partitionID64)

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
		listedOffsetsByTopic, err := (*kadm.Client).ListCommittedOffsets(adminClient, ctx, topicName)
		cobra.CheckErr(err)
		listedOffsets := listedOffsetsByTopic[args[0]]
		if len(listedOffsets) == 0 {
			fmt.Printf("No partitions found for topic '%s'. Does it exist?\n", args[0])
			return
		}

		partitionListedOffset, ok := listedOffsets[partitionID]
		if !ok {
			fmt.Printf("No partition found with ID '%d' for topic '%s'. Does it exist?\n", partitionID, args[0])
			return
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

		fmt.Printf("%-30s%s\n", "Topic: ", partitionDetail.Topic)
		fmt.Printf("%-30s%d\n", "Partition: ", partitionDetail.Partition)
		fmt.Printf("%-30s%d\n", "Latest commited offset: ", partitionListedOffset.Offset)
		fmt.Printf("%-30s%s\n", "Latest commited timestamp: ", formattedTimestamp)
		fmt.Printf("%-30s%d\n", "Leader: ", partitionDetail.Leader)
		fmt.Printf("%-30s%d\n", "Leader epoch: ", partitionDetail.LeaderEpoch)
		fmt.Printf("%-30s%v\n", "Replicas: ", partitionDetail.Replicas)
		fmt.Printf("%-30s%v\n", "Synced replicas: ", partitionDetail.ISR)
		fmt.Printf("%-30s%v\n", "Offline replicas: ", partitionDetail.OfflineReplicas)
	},
}

func init() {
	describeCmd.AddCommand(partitionCmd)
}
