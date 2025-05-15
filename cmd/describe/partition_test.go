package describe

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

func TestDescribePartition(t *testing.T) {
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
		partitions       int
		replicas         int
		topicOptions     map[string]*string
		produceMessages  map[string]int // topic name -> number of messages to produce
		describeArgs     []string
		expectedError    bool
		expectedOutput   []string
		unexpectedOutput []string
	}{
		{
			name:          "describe single partition",
			createTopics:  []string{"test-topic"},
			partitions:    1,
			replicas:      1,
			describeArgs:  []string{"describe", "partition", "test-topic", "0"},
			expectedError: false,
			expectedOutput: []string{
				`Topic:                        test-topic
Partition:                    0
Latest commited offset:       0
Latest commited timestamp:    -1
Leader:                       1
Leader epoch:                 0
Replicas:                     [1]
Synced replicas:              [1]
Offline replicas:             []
`,
			},
		},
		{
			name:          "describe topic with multiple partitions",
			createTopics:  []string{"multi-partition-topic"},
			partitions:    3,
			replicas:      1,
			describeArgs:  []string{"describe", "partition", "multi-partition-topic", "1"},
			expectedError: false,
			expectedOutput: []string{
				"Topic:                        multi-partition-topic",
				"Partition:                    1",
				"Latest commited offset:       0",
			},
		},
		{
			name:          "describe non-existent topic",
			createTopics:  []string{"existing-topic"},
			partitions:    1,
			replicas:      1,
			describeArgs:  []string{"describe", "partition", "non-existent-topic", "0"},
			expectedError: true,
			expectedOutput: []string{
				"Error: error listing offsets for topic 'non-existent-topic': UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.",
			},
		},
		{
			name:          "describe non-existent partition",
			createTopics:  []string{"single-partition-topic"},
			partitions:    1,
			replicas:      1,
			describeArgs:  []string{"describe", "partition", "single-partition-topic", "5"},
			expectedError: true,
			expectedOutput: []string{
				"no partition found with ID '5' for topic 'single-partition-topic'",
			},
		},
		{
			name:            "describe partition with messages",
			createTopics:    []string{"messages-topic"},
			partitions:      1,
			replicas:        1,
			produceMessages: map[string]int{"messages-topic": 10},
			describeArgs:    []string{"describe", "partition", "messages-topic", "0"},
			expectedError:   false,
			expectedOutput: []string{
				"Topic:                        messages-topic",
				"Partition:                    0",
				"Latest commited offset:       10",
			},
		},
		{
			name:          "invalid partition ID",
			createTopics:  []string{"test-topic"},
			partitions:    1,
			replicas:      1,
			describeArgs:  []string{"describe", "partition", "test-topic", "abc"},
			expectedError: true,
			expectedOutput: []string{
				"<partition_id> must be a number",
			},
		},
		{
			name:          "missing arguments",
			createTopics:  []string{"test-topic"},
			partitions:    1,
			replicas:      1,
			describeArgs:  []string{"describe", "partition", "test-topic"},
			expectedError: true,
			expectedOutput: []string{
				"Error: accepts 2 arg(s), received 1",
			},
		},
		{
			name:          "too many arguments",
			createTopics:  []string{"test-topic"},
			partitions:    1,
			replicas:      1,
			describeArgs:  []string{"describe", "partition", "test-topic", "0", "extra"},
			expectedError: true,
			expectedOutput: []string{
				"Error: accepts 2 arg(s), received 3",
			},
		},
		{
			name:            "multi-partition topic with specific partition details",
			createTopics:    []string{"multi-part-topic"},
			partitions:      3,
			replicas:        1,
			produceMessages: map[string]int{"multi-part-topic": 15},
			describeArgs:    []string{"describe", "partition", "multi-part-topic", "0"},
			expectedError:   false,
			expectedOutput: []string{
				"Topic:                        multi-part-topic",
				"Partition:                    0",
				"Latest commited offset:       15",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, testConfig)
			defer test_helpers.CleanupTestConfig(t, tempDir)
			defer test_helpers.CleanupKafkaCluster(t, cl, adminClient, ctx)

			// Create topics for testing
			if len(tt.createTopics) > 0 {
				options := tt.topicOptions
				if options == nil {
					options = make(map[string]*string)
				}
				_, err := adminClient.CreateTopics(ctx, int32(tt.partitions), int16(tt.replicas), options, tt.createTopics...)
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

			if len(tt.produceMessages) > 0 {
				for topic, count := range tt.produceMessages {
					test_helpers.ProduceMessages(t, cl, topic, count)
				}
			}

			output, err := test_helpers.ExecuteCommandWrapper(tt.describeArgs)

			for _, expected := range tt.expectedOutput {
				assert.Contains(t, output, expected, "Expected output to contain %q", expected)
			}

			for _, unexpected := range tt.unexpectedOutput {
				assert.NotContains(t, output, unexpected, "Output should not contain %q", unexpected)
			}

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
