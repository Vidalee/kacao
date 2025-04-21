package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
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
	Run: func(command *cobra.Command, args []string) {
		if len(args) == 0 {
			err := command.Help()
			cobra.CheckErr(err)
			return
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
				fmt.Printf("Error: Topic '%s' does not exist in the cluster!\n", topicToDelete)
				os.Exit(1)
			}
		}

		topicDeleteResponses, err := (*kadm.Client).DeleteTopics(adminClient, ctx, args...)
		if err != nil {
			fmt.Printf("Error: Failed to delete topics: %v\n", err)
			os.Exit(1)
		}
		for _, topicDeleteResponse := range topicDeleteResponses {
			if topicDeleteResponse.Err != nil {
				fmt.Printf("Failed to delete topic '%s': %v: %s\n", topicDeleteResponse.Topic, topicDeleteResponse.Err, topicDeleteResponse.ErrMessage)
			} else {
				fmt.Printf("Deleted topic '%s'\n", topicDeleteResponse.Topic)
			}
		}
	},
}

func init() {
	deleteCmd.AddCommand(topicCmd)
}
