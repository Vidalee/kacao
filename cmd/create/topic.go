package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
)

var deleteTopicCmd = &cobra.Command{
	Use:   "topic <topic_name> [--partitions <partitions>] [--replication-factor <replication_factor>]",
	Short: "Create a topic",
	Long: `Create one or many topic.

Create a single topic:
- kacao create topic <topic_name> [--partitions <partitions>] [--replication-factor <replication_factor>]

If you create multiple topics at once, they will all have the same number of partitions and replication factor:
- kacao create topic <topic_name_1> <topic_name_2> ... [--partitions <partitions>] [--replication-factor <replication_factor>]`,
	Run: func(command *cobra.Command, args []string) {
		fmt.Printf("%d args\n", len(args))
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

		topicName := args[0]
		partitions, err := command.Flags().GetInt32("partitions")
		cobra.CheckErr(err)
		replicationFactor, err := command.Flags().GetInt16("replication-factor")
		cobra.CheckErr(err)

		if partitions < 1 {
			createTopicResponse, err := (*kadm.Client).CreateTopic(adminClient, ctx, partitions, replicationFactor, map[string]*string{}, topicName)
			cobra.CheckErr(err)

			if createTopicResponse.Err != nil {
				fmt.Printf("Error creating topic '%s': %s: %s\n", topicName, createTopicResponse.Err, createTopicResponse.ErrMessage)
				os.Exit(1)
			}
			fmt.Printf("Created topic '%s'.\n", topicName)
		} else {
			createTopicsResponses, err := (*kadm.Client).CreateTopics(adminClient, ctx, partitions, replicationFactor, map[string]*string{}, args...)
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
	deleteTopicCmd.Flags().Int32("partitions", 1, "Number of partitions")
	deleteTopicCmd.Flags().Int16("replication-factor", 1, "Replication factor")

	createCmd.AddCommand(deleteTopicCmd)
}
