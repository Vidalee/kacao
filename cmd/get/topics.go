package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"slices"
)

var topicsCmd = &cobra.Command{
	Use:   "topics",
	Short: "Display topics of the current cluster",
	Long: `Display topics of the current cluster

You can also specify topics name to only display that topic:
- kacao get topics <topic_name>
- kacao get topics <topic_name_1> <topic_name_2> ...`,
	RunE: func(command *cobra.Command, args []string) error {
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

		var topicDetails kadm.TopicDetails
		internal, err := command.Flags().GetBool("internal")
		cobra.CheckErr(err)
		if internal {
			topicDetails, err = (*kadm.Client).ListTopicsWithInternal(adminClient, ctx)
		} else {
			topicDetails, err = (*kadm.Client).ListTopics(adminClient, ctx)
		}
		cobra.CheckErr(err)

		_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%-50s%-25s%-25s%-25s\n", "Topic", "Topic ID", "Partitions", "Replicas", "Is internal")
		cobra.CheckErr(err)

		for _, topicDetail := range topicDetails {
			if len(args) > 0 {
				if !slices.Contains(args, topicDetail.Topic) {
					continue
				}
			}

			_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%-50x%-25d%-25d%-25v\n",
				topicDetail.Topic,
				topicDetail.ID,
				len(topicDetail.Partitions),
				len(topicDetail.Partitions[0].Replicas),
				topicDetail.IsInternal,
			)
			cobra.CheckErr(err)
		}

		return nil
	},
}

func init() {
	topicsCmd.Flags().BoolP("internal", "i", false, "Show internal topics")
	getCmd.AddCommand(topicsCmd)
}
