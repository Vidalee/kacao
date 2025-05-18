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
	"time"
)

func TestGetMessages(t *testing.T) {
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
		produceMessages  map[string][]string
		headers          []map[string]string
		keys             []string
		getArgs          []string
		expectedPatterns []*regexp.Regexp
		expectedError    bool
	}{
		{
			name:         "existent topic with messages",
			createTopics: []string{"topic1"},
			produceMessages: map[string][]string{
				"topic1": {"test message 0", "test message 1", "test message 2"},
			},
			headers: []map[string]string{
				{"header-key0": "header-value0"},
				{"header-key1": "header-value1"},
				{"header-key2": "header-value2"},
			},
			keys:    []string{"key0", "key1", "key2"},
			getArgs: []string{"get", "messages", "topic1"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Partition\s+Offset\s+Key\s+Value\s+Headers\s+topic1\s+0\s+2\s+key2\s+test message 2\s+header-key2: header-value2\s+topic1\s+0\s+1\s+key1\s+test message 1\s+header-key1: header-value1\s+topic1\s+0\s+0\s+key0\s+test message 0\s+header-key0: header-value0`),
			},
			expectedError: false,
		},
		{
			name:         "existent topic with messages and no headers nor key",
			createTopics: []string{"topic2"},
			produceMessages: map[string][]string{
				"topic2": {"test message 0"},
			},
			headers: []map[string]string{
				{},
			},
			keys:    []string{""},
			getArgs: []string{"get", "messages", "topic2"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Partition\s+Offset\s+Key\s+Value\s+Headers\s+topic2\s+0\s+0\s+\s+test message 0`),
			},
			expectedError: false,
		},
		{
			name:         "non-existent topic",
			createTopics: []string{"topic3"},
			getArgs:      []string{"get", "messages", "non-existent-topic"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Error: error listing offsets for topic 'non-existent-topic': UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition\.`),
			},
			expectedError: true,
		},
		{
			name:         "filter by header key=value",
			createTopics: []string{"topic4"},
			produceMessages: map[string][]string{
				"topic4": {"test message 0", "test message 1", "test message 2"},
			},
			headers: []map[string]string{
				{"header-key0": "header-value0"},
				{"header-key1": "header-value1"},
				{"header-key2": "header-value2"},
			},
			keys:    []string{"key0", "key1", "key2"},
			getArgs: []string{"get", "messages", "topic4", "--header", "header-key1=header-value1"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Partition\s+Offset\s+Key\s+Value\s+Headers\s+topic4\s+0\s+1\s+key1\s+test message 1\s+header-key1: header-value1`),
			},
			expectedError: false,
		},
		{
			name:         "filter by header key=* and key",
			createTopics: []string{"topic5"},
			produceMessages: map[string][]string{
				"topic5": {"test message 0", "test message 1", "test message 2"},
			},
			headers: []map[string]string{
				{"header-key0": "header-value0"},
				{"wildcard": "header-value1"},
				{"wildcard": "header-value2"},
			},
			keys:    []string{"key0", "key1", "key2"},
			getArgs: []string{"get", "messages", "topic5", "--header", "wildcard=*", "--key", "key1"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Topic\s+Partition\s+Offset\s+Key\s+Value\s+Headers\s+topic5\s+0\s+1\s+key1\s+test message 1\s+wildcard: header-value1\s+`),
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
				_, err := adminClient.CreateTopics(ctx, 1, 1, nil, tt.createTopics...)
				assert.NoError(t, err)
			}

			if len(tt.produceMessages) > 0 {
				for topic, messages := range tt.produceMessages {
					for index, msg := range messages {
						test_helpers.ProduceMessageWithHeadersAndKey(t, cl, topic, msg, tt.keys[index], tt.headers[index])
					}
				}
			}

			// Needed since if we don't correctly close the client because of the test context
			// we will have issues consuming messages with the same consumer group in the next test
			//viper.Set("contexts.test-context.consumer-group", test_helpers.RandomString(7))

			// It doesn't fix the issue, but it's something I guess, here is a sleep to try to avoid the
			// "Error: UNKNOWN_MEMBER_ID: The coordinator is not aware of this member." with tests I guess :/
			time.Sleep(10 * time.Second)

			output, err := test_helpers.ExecuteCommandWrapper(tt.getArgs)

			for _, pattern := range tt.expectedPatterns {
				assert.Regexp(t, pattern, output)
			}

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
