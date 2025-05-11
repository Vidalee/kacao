package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func TestCreatePartition(t *testing.T) {
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
		name            string
		setupTopics     map[string]int32 // Topic name -> initial partition count
		args            []string
		expectedError   bool
		expectedOutput  string
		checkHelp       bool
		addedPartitions int
	}{
		{
			name: "successful partition addition",
			setupTopics: map[string]int32{
				"test-topic": 1,
			},
			args:            []string{"create", "partition", "test-topic", "--partitions", "3"},
			expectedError:   false,
			expectedOutput:  "Successfully created 3 partitions for topic 'test-topic'",
			addedPartitions: 3,
		},
		{
			name: "add partitions to multiple topics",
			setupTopics: map[string]int32{
				"test-topic-1": 1,
				"test-topic-2": 2,
			},
			args:            []string{"create", "partition", "test-topic-1", "test-topic-2", "--partitions", "4"},
			expectedError:   false,
			expectedOutput:  "Successfully created 4 partitions",
			addedPartitions: 4,
		},
		{
			name:           "invalid command with no topic name",
			args:           []string{"create", "partition"},
			expectedError:  false,
			expectedOutput: "Usage:",
			checkHelp:      true,
		},
		{
			name:           "non-existent topic",
			args:           []string{"create", "partition", "non-existent-topic", "--partitions", "3"},
			expectedError:  true,
			expectedOutput: "does not exist in the cluster",
		},
		{
			name: "add zero partitions",
			setupTopics: map[string]int32{
				"test-topic-zero": 1,
			},
			args:           []string{"create", "partition", "test-topic-zero", "--partitions", "0"},
			expectedError:  true,
			expectedOutput: "Error creating partitions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, testConfig)
			defer test_helpers.CleanupTestConfig(t, tempDir)
			defer test_helpers.CleanupKafkaCluster(t, cl, adminClient, ctx)

			for topicName, initialPartitions := range tt.setupTopics {
				_, err := adminClient.CreateTopics(ctx, initialPartitions, 1, nil, topicName)
				assert.NoError(t, err, "Failed to create initial topic %s with %d partitions", topicName, initialPartitions)

				topics, err := adminClient.ListTopics(ctx)
				assert.NoError(t, err)
				var found bool
				for _, topicDetail := range topics {
					if topicDetail.Topic == topicName {
						found = true
						assert.Equal(t, len(topicDetail.Partitions), int(initialPartitions),
							"Topic %s should have %d partitions before test", topicName, initialPartitions)
						break
					}
				}
				assert.True(t, found, "Topic %s should exist", topicName)
			}

			output, err := test_helpers.ExecuteCommandWrapper(tt.args)

			if tt.expectedError {
				assert.Error(t, err)
				assert.Contains(t, output, tt.expectedOutput)
				return
			}

			assert.NoError(t, err)
			assert.Contains(t, output, tt.expectedOutput)

			if tt.addedPartitions > 0 {
				topics, err := adminClient.ListTopics(ctx)
				assert.NoError(t, err)
				for topicName := range tt.setupTopics {
					var found bool
					for _, topicDetail := range topics {
						if topicDetail.Topic == topicName {
							found = true
							assert.Equal(t, len(topicDetail.Partitions), int(tt.setupTopics[topicName])+tt.addedPartitions,
								"Topic %s should have %d partitions after test but had %d",
								topicName, int(tt.setupTopics[topicName])+tt.addedPartitions, tt.addedPartitions)
							break
						}
					}
					assert.True(t, found, "Topic %s should exist", topicName)
				}
			}
		})
	}
}
