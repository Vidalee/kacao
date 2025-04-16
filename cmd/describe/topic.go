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

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Describe a topic of the current cluster",
	Long: `Describe a topic of the current cluster

- kacao get topics <topic_name>
- kacao get topics <topic_name_1> <topic_name_2> ...`,
	Run: func(command *cobra.Command, args []string) {
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
				fmt.Printf("Error: Topic '%s' does not exist in the cluster!\n", topicToDescribe)
				os.Exit(1)
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
				fmt.Printf("Error retrieving topic %s in map\n", topicToDescribe)
				os.Exit(1)
			}

			fmt.Printf("%-25s%s\n", "Name: ", topicDetail.Topic)
			fmt.Printf("%-25s%x\n", "ID: ", topicDetail.ID)
			fmt.Printf("%-25s%d\n", "Partitions: ", len(topicDetail.Partitions))
			fmt.Printf("%-25s%d\n", "Replicas: ", len(topicDetail.Partitions[0].Replicas))
			fmt.Printf("%-25s%v\n", "Is internal: ", topicDetail.IsInternal)

			messageCount := getMessageCount(listedStartOffsets[topicToDescribe], listedEndOffsets[topicToDescribe])
			fmt.Printf("%-25s%d\n", "Message count: ", messageCount)

			resourceConfig, ok := resourceConfigMap[topicDetail.Topic]
			if !ok {
				fmt.Printf("No resource config found for topic '%s'.\n", topicDetail.Topic)
				continue
			}
			fmt.Printf("Resource Configs:\n")
			for _, config := range resourceConfig.Configs {
				if config.Sensitive {
					fmt.Printf("  %s: %s (sensitive)\n", config.Key, *config.Value)
				} else {
					fmt.Printf("  %s: %s\n", config.Key, *config.Value)
				}
			}
		}
	},
}

func getMessageCount(startOffsets map[int32]kadm.ListedOffset, endOffsets map[int32]kadm.ListedOffset) int64 {
	var count int64 = 0
	for partition, startOffset := range startOffsets {
		endOffset, ok := endOffsets[partition]
		if !ok {
			fmt.Printf("Error: No end offset found for partition %d\n", partition)
			continue
		}
		if startOffset.Err != nil {
			fmt.Printf("Error: Failed to get start offset for partition %d: %v\n", partition, startOffset.Err)
			continue
		}
		if endOffset.Err != nil {
			fmt.Printf("Error: Failed to get end offset for partition %d: %v\n", partition, endOffset.Err)
			continue
		}
		count += endOffset.Offset - startOffset.Offset
	}
	return count
}

func init() {
	describeCmd.AddCommand(topicCmd)
}
