package describe

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Describe a topic of the current cluster",
	Long: `Describe a topic of the current cluster

- kacao describe topic <topic_name>
- kacao describe topic <topic_name_1> <topic_name_2> ...`,
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

		topicDetails, err := (*kadm.Client).ListTopics(adminClient, ctx)
		cobra.CheckErr(err)

		topicsInCluster := make([]string, len(topicDetails))
		for _, topicDetail := range topicDetails {
			topicsInCluster = append(topicsInCluster, topicDetail.Topic)
		}

		topicsToDescribeMap := make(map[string]kadm.TopicDetail)

		for _, topicToDescribe := range args {
			found := false
			for _, topicDetail := range topicDetails {
				if topicDetail.Topic == topicToDescribe {
					topicsToDescribeMap[topicToDescribe] = topicDetail
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("topic '%s' does not exist in the cluster!\n", topicToDescribe)
			}
		}

		resourceConfigs, err := (*kadm.Client).DescribeTopicConfigs(adminClient, ctx, args...)
		cobra.CheckErr(err)
		resourceConfigMap := make(map[string]kadm.ResourceConfig)
		for _, resourceConfig := range resourceConfigs {
			resourceConfigMap[resourceConfig.Name] = resourceConfig
		}
		listedStartOffsets, err := (*kadm.Client).ListStartOffsets(adminClient, ctx, args...)
		cobra.CheckErr(err)
		listedEndOffsets, err := (*kadm.Client).ListEndOffsets(adminClient, ctx, args...)
		cobra.CheckErr(err)

		for _, topicToDescribe := range args {
			topicDetail, ok := topicsToDescribeMap[topicToDescribe]
			// Should never happen
			if !ok {
				return fmt.Errorf("retrieving topic %s in map\n", topicToDescribe)
			}

			_, err := fmt.Fprintf(command.OutOrStdout(), "%-25s%s\n", "Name: ", topicDetail.Topic)
			cobra.CheckErr(err)
			_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%x\n", "ID: ", topicDetail.ID)
			cobra.CheckErr(err)
			_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%d\n", "Partitions: ", len(topicDetail.Partitions))
			cobra.CheckErr(err)
			_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%d\n", "Replicas: ", len(topicDetail.Partitions[0].Replicas))
			cobra.CheckErr(err)
			_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%v\n", "Is internal: ", topicDetail.IsInternal)
			cobra.CheckErr(err)

			messageCount := getMessageCount(listedStartOffsets[topicToDescribe], listedEndOffsets[topicToDescribe])
			_, err = fmt.Fprintf(command.OutOrStdout(), "%-25s%d\n", "Message count: ", messageCount)
			cobra.CheckErr(err)

			resourceConfig, ok := resourceConfigMap[topicDetail.Topic]
			if !ok {
				_, err = fmt.Fprintf(command.OutOrStdout(), "No resource config found for topic '%s'.\n", topicDetail.Topic)
				cobra.CheckErr(err)
				continue
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "Resource Configs:\n")
			cobra.CheckErr(err)
			for _, config := range resourceConfig.Configs {
				if config.Sensitive {
					_, err = fmt.Fprintf(command.OutOrStdout(), "  %s: %s (sensitive)\n", config.Key, *config.Value)
				} else {
					_, err = fmt.Fprintf(command.OutOrStdout(), "  %s: %s\n", config.Key, *config.Value)
				}
				cobra.CheckErr(err)
			}
		}
		return nil
	},
}

func getMessageCount(startOffsets map[int32]kadm.ListedOffset, endOffsets map[int32]kadm.ListedOffset) int64 {
	var count int64 = 0
	for partition, startOffset := range startOffsets {
		endOffset, ok := endOffsets[partition]
		if !ok {
			err := fmt.Errorf("no end offset found for partition %d\n", partition)
			cobra.CheckErr(err)
			continue
		}
		if startOffset.Err != nil {
			err := fmt.Errorf("Error: Failed to get start offset for partition %d: %v\n", partition, startOffset.Err)
			cobra.CheckErr(err)
			continue
		}
		if endOffset.Err != nil {
			err := fmt.Errorf("Error: Failed to get end offset for partition %d: %v\n", partition, endOffset.Err)
			cobra.CheckErr(err)
			continue
		}
		count += endOffset.Offset - startOffset.Offset
	}
	return count
}

func init() {
	describeCmd.AddCommand(topicCmd)
}
