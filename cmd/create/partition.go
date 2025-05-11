package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"maps"
	"slices"
)

var partitionCmd = &cobra.Command{
	Use:   "partition <topic_name> [--partitions <partitions>]",
	Short: "Add partitions to a topic",
	Long: `Add partitions to a topic.

Add two partitions to a topic:
- kacao create partition <topic_name> --partitions 2

You can also specify multiple topics to add partitions to:
- kacao create partition <topic_name_1> <topic_name_2> --partitions 2`,
	RunE: func(command *cobra.Command, args []string) error {
		if len(args) == 0 {
			return command.Help()
		}

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

		topicDetails, err := (*kadm.Client).ListTopicsWithInternal(adminClient, ctx)
		cobra.CheckErr(err)
		clusterTopicsSlice := make([]string, 0, len(topicDetails))

		for clusterTopic := range maps.Keys(topicDetails) {
			clusterTopicsSlice = append(clusterTopicsSlice, clusterTopic)
		}

		for _, topicToAddPartitions := range args {
			if !slices.Contains(clusterTopicsSlice, topicToAddPartitions) {
				return fmt.Errorf("topic '%s' does not exist in the cluster!\n", topicToAddPartitions)
			}
		}

		partitions, err := command.Flags().GetInt("partitions")
		cobra.CheckErr(err)

		createPartitionsResponse, err := (*kadm.Client).CreatePartitions(adminClient, ctx, partitions, args...)
		cobra.CheckErr(err)

		failedToCreateAnyPartitions := false
		for topic, response := range createPartitionsResponse {
			if response.Err != nil {
				_, err := fmt.Fprintf(command.OutOrStdout(), "Error creating partitions for topic '%s': %v: %s\n", topic, response.Err, response.ErrMessage)
				cobra.CheckErr(err)
				failedToCreateAnyPartitions = true
				continue
			}
			_, err := fmt.Fprintf(command.OutOrStdout(), "Successfully created %d partitions for topic '%s'.\n", partitions, topic)
			cobra.CheckErr(err)
		}

		if failedToCreateAnyPartitions {
			return fmt.Errorf("failed to create partitions for some topics")
		}
		return nil
	},
}

func init() {
	partitionCmd.Flags().Int("partitions", 1, "Number of partitions to add")

	createCmd.AddCommand(partitionCmd)
}
