package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"strings"
)

var topicCmd = &cobra.Command{
	Use:   "topic <topic_name> [--partitions <partitions>] [--replication-factor <replication_factor>] [--options <key=value>]",
	Short: "Create a topic",
	Long: `Create one or many topic.

Create a single topic:
- kacao create topic <topic_name> [--partitions <partitions>] [--replication-factor <replication_factor>] [--options <key=value>]

If you create multiple topics at once, they will all have the same number of partitions and replication factor:
- kacao create topic <topic_name_1> <topic_name_2> ... [--partitions <partitions>] [--replication-factor <replication_factor>] [--options <key=value>]`,
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

		partitions, err := command.Flags().GetInt32("partitions")
		cobra.CheckErr(err)
		replicationFactor, err := command.Flags().GetInt16("replication-factor")
		cobra.CheckErr(err)
		topicOptions := make(map[string]*string)
		topicOptionsArray, err := command.Flags().GetStringArray("options")
		cobra.CheckErr(err)
		for _, option := range topicOptionsArray {
			parts := strings.Split(option, "=")
			if len(parts) != 2 {
				return fmt.Errorf("invalid option format: %s. Expected key=value", option)
			}
			topicOptions[parts[0]] = &parts[1]
		}

		createTopicsResponses, err := adminClient.CreateTopics(ctx, partitions, replicationFactor, topicOptions, args...)
		if err != nil {
			return fmt.Errorf("error creating topic(s) '%s': %s", strings.Join(args, ", "), err)
		}

		failedToCreateAnyTopic := false
		for _, createTopicResponse := range createTopicsResponses {
			if createTopicResponse.Err != nil {
				_, err := fmt.Fprintf(command.OutOrStdout(), "Error creating topic '%s': %s: %s\n", createTopicResponse.Topic, createTopicResponse.Err, createTopicResponse.ErrMessage)
				cobra.CheckErr(err)
				failedToCreateAnyTopic = true
			} else {
				_, err := fmt.Fprintf(command.OutOrStdout(), "Created topic '%s'\n", createTopicResponse.Topic)
				cobra.CheckErr(err)
			}
		}

		if failedToCreateAnyTopic {
			return fmt.Errorf("failed to create one or more topics")
		}
		return nil
	},
}

func init() {
	topicCmd.Flags().Int32("partitions", 1, "Number of partitions")
	topicCmd.Flags().Int16("replication-factor", 1, "Replication factor")
	topicCmd.Flags().StringArrayP("options", "o", []string{}, "Topic options in the form of key=value. Can be specified multiple times. Example: --options retention.ms=1000 --options cleanup.policy=compact")

	createCmd.AddCommand(topicCmd)
}
