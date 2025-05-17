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

func TestGetPartitions(t *testing.T) {
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
		produceMessages  map[string]int
		getArgs          []string
		expectedPatterns []*regexp.Regexp
		expectedError    bool
	}{
		{
			name:          "no arguments provided",
			createTopics:  []string{},
			getArgs:       []string{"get", "partitions"},
			expectedError: true,
		},
		{
			name:         "get partitions for single topic",
			createTopics: []string{"topic1"},
			partitions:   1,
			produceMessages: map[string]int{
				"topic1": 3,
			},
			getArgs: []string{"get", "partitions", "topic1"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Partition\s+Record Offset\s+Leader Epoch\s+Timestamp\s+`),
				regexp.MustCompile(`topic1\s+0\s+3\s+0+\s+-1\s+`),
			},
			expectedError: false,
		},
		{
			name:         "get partitions for non-existent topic",
			createTopics: []string{"topic2"},
			getArgs:      []string{"get", "partitions", "non-existent-topic"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Error: error listing offsets for topic 'non-existent-topic': UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.`),
			},
			expectedError: true,
		},
		{
			name:         "get partitions for a topic with multiple partitions",
			createTopics: []string{"multi-partition-topic"},
			partitions:   3,
			produceMessages: map[string]int{
				"multi-partition-topic": 5,
			},
			getArgs: []string{"get", "partitions", "multi-partition-topic"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Partition\s+Record Offset\s+Leader Epoch\s+Timestamp\s+`),
				regexp.MustCompile(`multi-partition-topic\s+0\s+\d\s+0+\s+-1\s+`),
				regexp.MustCompile(`multi-partition-topic\s+1\s+\d\s+0+\s+-1\s+`),
				regexp.MustCompile(`multi-partition-topic\s+2\s+\d\s+0+\s+-1\s+`),
				regexp.MustCompile(`multi-partition-topic\s+\d\s+5\s+0\s+-1+\s+.*`),
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, testConfig)
			defer test_helpers.CleanupTestConfig(t, tempDir)
			defer test_helpers.CleanupKafkaCluster(t, cl, adminClient, ctx)

			if len(tt.createTopics) > 0 {
				_, err := adminClient.CreateTopics(ctx, int32(tt.partitions), 1, nil, tt.createTopics...)
				assert.NoError(t, err, "Failed to create test topics")
			}

			if len(tt.produceMessages) > 0 {
				for topic, count := range tt.produceMessages {
					test_helpers.ProduceMessages(t, cl, topic, count)
				}
			}

			output, err := test_helpers.ExecuteCommandWrapper(tt.getArgs)

			for _, pattern := range tt.expectedPatterns {
				assert.True(t, pattern.MatchString(output), "Expected output to match pattern: %s\nGot: %s", pattern.String(), output)
			}

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
