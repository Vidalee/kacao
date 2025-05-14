package describe

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

func TestDescribeTopic(t *testing.T) {
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
		topicOptions     map[string]*string
		produceMessages  map[string]int // topic name -> number of messages to produce
		describeArgs     []string
		expectedError    bool
		expectedOutput   []string
		unexpectedOutput []string
	}{
		{
			name:           "describe single topic",
			createTopics:   []string{"test-topic"},
			describeArgs:   []string{"describe", "topic", "test-topic"},
			expectedError:  false,
			expectedOutput: []string{"Name: ", "test-topic", "Partitions: ", "1", "Replicas: ", "Message count: ", "0"},
		},
		{
			name:           "describe multiple topics",
			createTopics:   []string{"topic1", "topic2"},
			describeArgs:   []string{"describe", "topic", "topic1", "topic2"},
			expectedError:  false,
			expectedOutput: []string{"Name: ", "topic1", "Name: ", "topic2", "Partitions: ", "1", "Replicas: "},
		},
		{
			name:           "describe non-existent topic",
			createTopics:   []string{"existing-topic"},
			describeArgs:   []string{"describe", "topic", "non-existent-topic"},
			expectedError:  true,
			expectedOutput: []string{"topic 'non-existent-topic' does not exist in the cluster!"},
		},
		{
			name:         "describe topic with custom config",
			createTopics: []string{"config-topic"},
			topicOptions: map[string]*string{
				"cleanup.policy": stringPtr("compact"),
				"retention.ms":   stringPtr("86400000"),
			},
			describeArgs:  []string{"describe", "topic", "config-topic"},
			expectedError: false,
			expectedOutput: []string{
				"Name:                    config-topic",
				"Partitions:              1",
				"Resource Configs:",
				"cleanup.policy: compact",
				"retention.ms: 86400000",
			},
		},
		{
			name:            "describe topic with messages",
			createTopics:    []string{"messages-topic"},
			produceMessages: map[string]int{"messages-topic": 10},
			describeArgs:    []string{"describe", "topic", "messages-topic"},
			expectedError:   false,
			expectedOutput:  []string{"Name:                    messages-topic", "Message count:           10"},
		},
		{
			name:            "describe mixed topics with and without messages",
			createTopics:    []string{"with-messages", "without-messages"},
			produceMessages: map[string]int{"with-messages": 5},
			describeArgs:    []string{"describe", "topic", "with-messages", "without-messages"},
			expectedError:   false,
			expectedOutput: []string{
				"Name:                    with-messages",
				"Message count:           5",
				"Name:                    without-messages",
				"Message count:           0",
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
				_, err := adminClient.CreateTopics(ctx, 1, 1, options, tt.createTopics...)
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
					produceMessages(t, cl, topic, count)
				}
			}

			output, err := test_helpers.ExecuteCommandWrapper(tt.describeArgs)

			for _, expected := range tt.expectedOutput {
				assert.Contains(t, output, expected, "Expected output to contain %q", expected)
			}

			if tt.expectedError {
				assert.Error(t, err)
				return
			}

			for _, unexpected := range tt.unexpectedOutput {
				assert.NotContains(t, output, unexpected, "Output should not contain %q", unexpected)
			}
		})
	}
}

func produceMessages(t *testing.T, cl *kgo.Client, topic string, count int) {
	t.Helper()

	records := make([]*kgo.Record, count)
	for i := 0; i < count; i++ {
		records[i] = &kgo.Record{
			Topic: topic,
			Value: []byte(fmt.Sprintf("test message %d", i)),
		}
	}

	results := cl.ProduceSync(context.Background(), records...)
	for _, result := range results {
		assert.NoError(t, result.Err, "Failed to produce message to topic %s", topic)
	}
}

func stringPtr(s string) *string {
	return &s
}
