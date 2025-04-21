package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
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

		topicName := args[0]
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
				fmt.Printf("Invalid option format: %s. Expected key=value.\n", option)
				os.Exit(1)
			}
			topicOptions[parts[0]] = &parts[1]
		}

		if partitions < 1 {
			createTopicResponse, err := (*kadm.Client).CreateTopic(adminClient, ctx, partitions, replicationFactor, topicOptions, topicName)
			cobra.CheckErr(err)

			if createTopicResponse.Err != nil {
				fmt.Printf("Error creating topic '%s': %s: %s\n", topicName, createTopicResponse.Err, createTopicResponse.ErrMessage)
				os.Exit(1)
			}
			fmt.Printf("Created topic '%s'.\n", topicName)
		} else {
			createTopicsResponses, err := (*kadm.Client).CreateTopics(adminClient, ctx, partitions, replicationFactor, topicOptions, args...)
			cobra.CheckErr(err)
			for _, createTopicResponse := range createTopicsResponses {
				if createTopicResponse.Err != nil {
					fmt.Printf("Error creating topic '%s': %s: %s\n", createTopicResponse.Topic, createTopicResponse.Err, createTopicResponse.ErrMessage)
				} else {
					fmt.Printf("Created topic '%s'.\n", createTopicResponse.Topic)
				}
			}
		}
	},
}

func init() {
	topicCmd.Flags().Int32("partitions", 1, "Number of partitions")
	topicCmd.Flags().Int16("replication-factor", 1, "Replication factor")
	topicCmd.Flags().StringArrayP("options", "o", []string{}, "Topic options in the form of key=value. Can be specified multiple times. Example: --options retention.ms=1000 --options cleanup.policy=compact")

	createCmd.AddCommand(topicCmd)
}
