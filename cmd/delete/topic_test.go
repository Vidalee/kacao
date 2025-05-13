package delete

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDeleteTopic(t *testing.T) {
	ctx := context.Background()

	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			fmt.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		fmt.Printf("failed to start container: %s", err)
		return
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	assert.NoError(t, err)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("kacao-test-group"),
	)
	assert.NoError(t, err)
	adminClient := kadm.NewClient(cl)
	defer cl.Close()
	defer adminClient.Close()

	testConfig := test_helpers.TestConfig{
		Clusters: map[string]map[string]interface{}{
			"test-cluster": {
				"bootstrap-servers": brokers,
			},
		},
		Contexts: map[string]map[string]interface{}{
			"test-context": {
				"cluster":        "test-cluster",
				"consumer-group": "test-group",
			},
		},
	}

	tests := []struct {
		name             string
		createTopics     []string
		deleteArgs       []string
		expectedError    bool
		expectedOutput   string
		checkHelp        bool
		expectedExisting []string
		expectedDeleted  []string
	}{
		{
			name:            "successful single topic deletion",
			createTopics:    []string{"test-topic"},
			deleteArgs:      []string{"delete", "topic", "test-topic"},
			expectedError:   false,
			expectedOutput:  "Deleted topic 'test-topic'",
			expectedDeleted: []string{"test-topic"},
		},
		{
			name:             "delete multiple topics",
			createTopics:     []string{"topic1", "topic2", "topic3"},
			deleteArgs:       []string{"delete", "topic", "topic1", "topic2"},
			expectedError:    false,
			expectedOutput:   "Deleted topic 'topic1'",
			expectedExisting: []string{"topic3"},
			expectedDeleted:  []string{"topic1", "topic2"},
		},
		{
			name:           "delete with no topic names",
			deleteArgs:     []string{"delete", "topic"},
			expectedError:  false,
			expectedOutput: "Usage:",
			checkHelp:      true,
		},
		{
			name:           "delete non-existent topic",
			deleteArgs:     []string{"delete", "topic", "non-existent-topic"},
			expectedError:  true,
			expectedOutput: "Error: topic 'non-existent-topic' does not exist in the cluster!",
		},
		{
			name:             "delete one topic and keep others",
			createTopics:     []string{"keep-topic", "delete-topic"},
			deleteArgs:       []string{"delete", "topic", "delete-topic"},
			expectedError:    false,
			expectedOutput:   "Deleted topic 'delete-topic'",
			expectedExisting: []string{"keep-topic"},
			expectedDeleted:  []string{"delete-topic"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, testConfig)
			defer test_helpers.CleanupTestConfig(t, tempDir)
			defer test_helpers.CleanupKafkaCluster(t, cl, adminClient, ctx)

			// Create topics for testing
			if len(tt.createTopics) > 0 {
				_, err := adminClient.CreateTopics(ctx, 1, 1, nil, tt.createTopics...)
				assert.NoError(t, err, "Failed to create test topics")

				topicsAfterCreate, err := adminClient.ListTopics(ctx)
				assert.NoError(t, err)

				for _, topicName := range tt.createTopics {
					topicExists := false
					for _, topicDetail := range topicsAfterCreate {
						if topicDetail.Topic == topicName {
							topicExists = true
							break
						}
					}
					assert.True(t, topicExists, "Topic '%s' was not created successfully", topicName)
				}
			}

			output, err := test_helpers.ExecuteCommandWrapper(tt.deleteArgs)
			if tt.expectedError {
				assert.Error(t, err)
				assert.Contains(t, output, tt.expectedOutput)
				return
			}

			assert.NoError(t, err)
			assert.Contains(t, output, tt.expectedOutput)
			if tt.checkHelp {
				return
			}

			topicsAfterDeletion, err := adminClient.ListTopics(ctx)
			assert.NoError(t, err)

			for _, expectedExistingTopic := range tt.expectedExisting {
				topicStillExists := false
				for _, topicDetail := range topicsAfterDeletion {
					if topicDetail.Topic == expectedExistingTopic {
						topicStillExists = true
						break
					}
				}
				assert.True(t, topicStillExists, "Topic '%s' should still exist after delete operation", expectedExistingTopic)
			}

			for _, expectedDeletedTopic := range tt.expectedDeleted {
				topicStillExists := false
				for _, topicDetail := range topicsAfterDeletion {
					if topicDetail.Topic == expectedDeletedTopic {
						topicStillExists = true
						break
					}
				}
				assert.False(t, topicStillExists, "Topic '%s' should have been deleted", expectedDeletedTopic)
			}
		})
	}
}
