package delete

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"slices"
)

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Delete a topic",
	Long: `Delete one or many topics.

This command will delete the specified topic(s) from the Kafka cluster. Use with caution as this action is irreversible:

- kacao delete topic <topic_name>
- kacao delete topic <topic_name_1> <topic_name_2> ...`,
	Example: "kacao delete topic <topic_name>",
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
		topicDetails, err := (*kadm.Client).ListTopics(adminClient, ctx)
		cobra.CheckErr(err)

		topicsInCluster := make([]string, len(topicDetails))
		for _, topicDetail := range topicDetails {
			topicsInCluster = append(topicsInCluster, topicDetail.Topic)
		}

		for _, topicToDelete := range args {
			if !slices.Contains(topicsInCluster, topicToDelete) {
				return fmt.Errorf("topic '%s' does not exist in the cluster!\n", topicToDelete)
			}
		}

		topicDeleteResponses, err := (*kadm.Client).DeleteTopics(adminClient, ctx, args...)
		if err != nil {
			return fmt.Errorf("failed to delete topics: %v\n", err)
		}
		for _, topicDeleteResponse := range topicDeleteResponses {
			if topicDeleteResponse.Err != nil {
				_, err := fmt.Fprintf(command.OutOrStdout(), "Failed to delete topic '%s': %v: %s\n", topicDeleteResponse.Topic, topicDeleteResponse.Err, topicDeleteResponse.ErrMessage)
				cobra.CheckErr(err)
			} else {
				_, err := fmt.Fprintf(command.OutOrStdout(), "Deleted topic '%s'\n", topicDeleteResponse.Topic)
				cobra.CheckErr(err)
			}
		}
		return nil
	},
}

func init() {
	deleteCmd.AddCommand(topicCmd)
}
