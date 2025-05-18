package produce

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
	"time"
)

func TestProduceMessages(t *testing.T) {
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
		produceArgs     []string
		expectedKey     string
		expectedHeaders map[string]string
		expectedMessage string
		expectedError   bool
	}{
		{
			name:            "basic produce message",
			produceArgs:     []string{"produce", "produce-topic-basic", "--message", "Hello World"},
			expectedMessage: "Hello World",
			expectedError:   false,
		},
		{
			name:            "produce message with key",
			produceArgs:     []string{"produce", "produce-topic-key", "--message", "Keyed Message", "--key", "my-key"},
			expectedKey:     "my-key",
			expectedMessage: "Keyed Message",
			expectedError:   false,
		},
		{
			name:            "produce message with headers",
			produceArgs:     []string{"produce", "produce-topic-header", "--message", "Header Message", "--header", "header-key=header-value"},
			expectedHeaders: map[string]string{"header-key": "header-value"},
			expectedMessage: "Header Message",
			expectedError:   false,
		},
		{
			name:            "produce message with key and headers",
			produceArgs:     []string{"produce", "produce-topic-key-header", "--message", "Keyed Header Message", "--key", "my-key", "--header", "header-key=header-value"},
			expectedKey:     "my-key",
			expectedHeaders: map[string]string{"header-key": "header-value"},
			expectedMessage: "Keyed Header Message",
			expectedError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, testConfig)
			defer test_helpers.CleanupTestConfig(t, tempDir)

			topicName := tt.produceArgs[1]
			cl, err := kgo.NewClient(
				kgo.SeedBrokers(brokers...),
				kgo.ConsumerGroup("kacao-test-group"),
				kgo.ConsumeTopics(topicName),
			)
			assert.NoError(t, err)
			adminClient := kadm.NewClient(cl)
			defer cl.Close()
			defer adminClient.Close()

			_, err = adminClient.CreateTopics(ctx, 1, 1, nil, topicName)
			assert.NoError(t, err)

			_, err = test_helpers.ExecuteCommandWrapper(tt.produceArgs)
			if tt.expectedError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			var records []kgo.Record
			timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			for {
				fetches := cl.PollFetches(timeoutCtx)
				if errs := fetches.Errors(); len(errs) > 0 {
					fmt.Printf("Fetch errors: %v", errs)
					break
				}

				iter := fetches.RecordIter()
				for !iter.Done() {
					record := *iter.Next()
					if record.Topic == topicName {
						records = append(records, record)
					}
				}

				if len(records) > 0 {
					break
				}
			}

			assert.Equal(t, tt.expectedMessage, string(records[0].Value))
			assert.Equal(t, tt.expectedKey, string(records[0].Key))

			if len(tt.expectedHeaders) > 0 {
				for _, header := range records[0].Headers {
					value, exists := tt.expectedHeaders[header.Key]
					if exists {
						assert.Equal(t, value, string(header.Value))
					}
				}
			}
		})
	}
}
