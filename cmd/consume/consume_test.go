package consume

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
	"sync"
	"testing"
	"time"
)

func TestConsumeMessages(t *testing.T) {
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
		kgo.SessionTimeout(120*time.Second),
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
		name                  string
		topicName             string
		produceMessages       []string
		getArgs               []string
		expectedPatterns      []*regexp.Regexp
		expectedError         bool
		produceAfterConsuming bool
	}{
		{
			name:            "consume messages earliest offset",
			topicName:       "consume-topic-earliest",
			produceMessages: []string{"Hello World 1", "Hello World 2", "Hello World 3"},
			getArgs:         []string{"consume", "consume-topic-earliest", "--offset", "earliest", "--timeout", "5"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Hello World 1\s+Hello World 2\s+Hello World 3`),
			},
			produceAfterConsuming: false,
		},
		{
			name:            "consume messages latest offset with delayed produce",
			topicName:       "consume-topic-latest",
			produceMessages: []string{"Delayed Message 1", "Delayed Message 2"},
			getArgs:         []string{"consume", "consume-topic-latest", "--offset", "latest", "--timeout", "10"},
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Delayed Message 1\s+Delayed Message 2`),
			},
			produceAfterConsuming: true,
		},
		{
			name:          "consume messages with negative timeout",
			topicName:     "consume-topic-negative-timeout",
			getArgs:       []string{"consume", "consume-topic-negative-timeout", "--offset", "latest", "--timeout", "-1"},
			expectedError: true,
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Error: invalid timeout value. Must be a non-negative integer representing seconds`),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, testConfig)
			defer test_helpers.CleanupTestConfig(t, tempDir)

			_, err = adminClient.CreateTopics(ctx, 1, 1, nil, tt.topicName)
			assert.NoError(t, err)

			var wg sync.WaitGroup
			if tt.produceAfterConsuming {
				wg.Add(1)
				go func() {
					defer wg.Done()
					time.Sleep(3 * time.Second)
					for _, msg := range tt.produceMessages {
						test_helpers.ProduceMessage(t, cl, tt.topicName, msg)
					}
				}()
			} else {
				for _, msg := range tt.produceMessages {
					test_helpers.ProduceMessage(t, cl, tt.topicName, msg)
				}
			}

			output, err := test_helpers.ExecuteCommandWrapper(tt.getArgs)
			for _, pattern := range tt.expectedPatterns {
				assert.Regexp(t, pattern, output)
			}

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			wg.Wait()
		})
	}
}
