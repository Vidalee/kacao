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

		topicDetails, err := (*kadm.Client).ListTopicsWithInternal(adminClient, ctx)
		cobra.CheckErr(err)
		clusterTopicsSlice := make([]string, 0, len(topicDetails))

		for clusterTopic := range maps.Keys(topicDetails) {
			clusterTopicsSlice = append(clusterTopicsSlice, clusterTopic)
		}

		for _, topicToAddPartitions := range args {
			if !slices.Contains(clusterTopicsSlice, topicToAddPartitions) {
				fmt.Printf("Error: Topic '%s' does not exist in the cluster!\n", topicToAddPartitions)
				return
			}
		}

		partitions, err := command.Flags().GetInt("partitions")
		cobra.CheckErr(err)

		createPartitionsResponse, err := (*kadm.Client).CreatePartitions(adminClient, ctx, partitions, args...)
		cobra.CheckErr(err)

		for topic, response := range createPartitionsResponse {
			if response.Err != nil {
				fmt.Printf("Error creating partitions for topic '%s': %v: %s\n", topic, response.Err, response.ErrMessage)
				continue
			}
			fmt.Printf("Successfully created %d partitions for topic '%s'.\n", partitions, topic)
		}
	},
}

func init() {
	partitionCmd.Flags().Int("partitions", 1, "Number of partitions to add")

	createCmd.AddCommand(partitionCmd)
}
