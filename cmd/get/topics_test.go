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
	"regexp"
	"testing"
)

func TestGetTopics(t *testing.T) {
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
		name              string
		createTopics      []string
		topicOptions      map[string]*string
		getArgs           []string
		expectedError     bool
		expectedPatterns  []*regexp.Regexp
		unexpectedTopics  []string
		consumeFromTopics []string
	}{
		{
			name:         "get all topics",
			createTopics: []string{"topic1", "topic2", "topic3"},
			getArgs:      []string{"get", "topics"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Topic ID\s+Partitions\s+Replicas\s+Is internal\s+`),
				regexp.MustCompile(`topic1\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+false\s+`),
				regexp.MustCompile(`topic2\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+false\s+`),
				regexp.MustCompile(`topic3\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+false\s+`),
			},
			expectedError: false,
		},
		{
			name:         "get specific topic",
			createTopics: []string{"topic1", "topic2", "topic3"},
			getArgs:      []string{"get", "topics", "topic2"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Topic ID\s+Partitions\s+Replicas\s+Is internal\s+`),
				regexp.MustCompile(`topic2\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+false\s+`),
			},
			unexpectedTopics: []string{"topic1", "topic3"},
			expectedError:    false,
		},
		{
			name:         "get multiple specific topics",
			createTopics: []string{"topic1", "topic2", "topic3", "topic4"},
			getArgs:      []string{"get", "topics", "topic1", "topic3"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Topic ID\s+Partitions\s+Replicas\s+Is internal\s+`),
				regexp.MustCompile(`topic1\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+false\s+`),
				regexp.MustCompile(`topic3\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+false\s+`),
			},
			unexpectedTopics: []string{"topic2", "topic4"},
			expectedError:    false,
		},
		{
			name:         "get non-existent topic",
			createTopics: []string{"topic1", "topic2"},
			getArgs:      []string{"get", "topics", "non-existent-topic"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Topic ID\s+Partitions\s+Replicas\s+Is internal\s+`),
			},
			unexpectedTopics: []string{"topic1", "topic2", "non-existent-topic"},
			expectedError:    false,
		},
		{
			name:         "get topics with internal flag",
			createTopics: []string{"user-topic1", "user-topic2"},
			getArgs:      []string{"get", "topics", "--internal"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Topic ID\s+Partitions\s+Replicas\s+Is internal\s+`),
				regexp.MustCompile(`user-topic1\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+false\s+`),
				regexp.MustCompile(`user-topic2\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+false\s+`),
				regexp.MustCompile(`__consumer_offsets\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+true\s+`),
			},
			consumeFromTopics: []string{"user-topic1"},
			expectedError:     false,
		},
		{
			name:         "get topics with short internal flag",
			createTopics: []string{"user-topic1", "user-topic2"},
			getArgs:      []string{"get", "topics", "-i"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Topic ID\s+Partitions\s+Replicas\s+Is internal\s+`),
				regexp.MustCompile(`user-topic1\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+false\s+`),
				regexp.MustCompile(`user-topic2\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+false\s+`),
				regexp.MustCompile(`__consumer_offsets\s+[0-9a-zA-Z+/=]+\s+1\s+1\s+true\s+`),
			},
			expectedError: false,
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

			// Consume messages from topics if specified (to create consumer offsets topics)
			if len(tt.consumeFromTopics) > 0 {
				for _, topic := range tt.consumeFromTopics {
					test_helpers.ProduceMessages(t, cl, topic, 1)

					consumerCl, err := kgo.NewClient(
						kgo.SeedBrokers(brokers...),
						kgo.ConsumerGroup("test-consumer-group"),
						kgo.ConsumeTopics(topic),
					)
					assert.NoError(t, err)

					for i := 0; i < 10; i++ { // Try a few times to ensure we get the message
						fetches := consumerCl.PollFetches(ctx)
						if fetches.NumRecords() > 0 {
							break
						}
					}

					consumerCl.Close()
				}
			}

			output, err := test_helpers.ExecuteCommandWrapper(tt.getArgs)

			for _, pattern := range tt.expectedPatterns {
				assert.True(t, pattern.MatchString(output), "Expected output to match pattern: %s\nGot: %s", pattern.String(), output)
			}

			for _, unexpectedTopic := range tt.unexpectedTopics {
				topicPattern := regexp.MustCompile(`\b` + unexpectedTopic + `\b`)
				assert.False(t, topicPattern.MatchString(output), "Unexpected topic '%s' found in output", unexpectedTopic)
			}

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
