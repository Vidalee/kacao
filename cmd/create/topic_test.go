package get

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

func TestCreateTopic(t *testing.T) {
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
		args              []string
		expectedError     bool
		expectedOutput    string
		checkHelp         bool
		topicNames        []string
		partitions        int
		replicationFactor int
		options           map[string]string
	}{
		{
			name:           "successful topic creation",
			args:           []string{"create", "topic", "test-topic", "--partitions", "1"},
			expectedError:  false,
			expectedOutput: "Created topic 'test-topic'",
			topicNames:     []string{"test-topic"},
			partitions:     1,
		},
		{
			name:           "successful topic creation with 3 partitions",
			args:           []string{"create", "topic", "test-topic", "--partitions", "3"},
			expectedError:  false,
			expectedOutput: "Created topic 'test-topic'",
			topicNames:     []string{},
			partitions:     3,
		},
		{
			name:           "multiple topics creation",
			args:           []string{"create", "topic", "test-topic", "test-topic-2", "test-topic-3"},
			expectedError:  false,
			expectedOutput: "Created topic 'test-topic'",
			topicNames:     []string{"test-topic", "test-topic-2", "test-topic-3"},
			partitions:     1,
		},
		{
			name:           "invalid topic creation with no name",
			args:           []string{"create", "topic"},
			expectedError:  false,
			expectedOutput: "Usage:",
			checkHelp:      true,
		},
		{
			name:           "successful topic creation with 2 partitions and options",
			args:           []string{"create", "topic", "test-topic", "--partitions", "2", "--options", "cleanup.policy=compact", "--options", "retention.ms=1000"},
			expectedError:  false,
			expectedOutput: "Created topic 'test-topic'",
			topicNames:     []string{"test-topic"},
			partitions:     2,
			options: map[string]string{
				"cleanup.policy": "compact",
				"retention.ms":   "1000",
			},
		},
		{
			name:          "successful topic creation with wrong option format options",
			args:          []string{"create", "topic", "test-topic", "--partitions", "2", "--options", "cleanup.policycompact", "--options", "retention.ms=1000"},
			expectedError: true,
			// Can't be accurate on this test because the value will be different when running the test alone or after the others
			// Since Cobra don't allow cleaning array and slice flags ^^
			expectedOutput: "invalid option format: ", //cleanup.policycompact. Expected key=value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, testConfig)
			defer test_helpers.CleanupTestConfig(t, tempDir)
			defer test_helpers.CleanupKafkaCluster(t, cl, adminClient, ctx)

			topicsBeforeCmd, err := adminClient.ListTopics(ctx)
			assert.NoError(t, err)

			topicExistsBefore := false
			var topicExistingBefore kadm.TopicDetail
			for _, topicDetail := range topicsBeforeCmd {
				for _, topicName := range tt.topicNames {
					if topicDetail.Topic == topicName {
						topicExistsBefore = true
						topicExistingBefore = topicDetail
						break
					}
				}
			}
			assert.False(t, topicExistsBefore, "Topic '%s' should not exist before test", topicExistingBefore.Topic)

			output, err := test_helpers.ExecuteCommandWrapper(tt.args)
			if tt.expectedError {
				assert.Error(t, err)
				assert.Contains(t, output, tt.expectedOutput)
				return
			}
			assert.NoError(t, err)
			if tt.checkHelp {
				assert.Contains(t, output, tt.expectedOutput)
				return
			}
			assert.Contains(t, output, tt.expectedOutput)

			topicsAfterCmd, err := adminClient.ListTopics(ctx)
			assert.NoError(t, err)

			var createdTopics []kadm.TopicDetail
			for _, topicDetail := range topicsAfterCmd {
				for _, topicName := range tt.topicNames {
					if topicDetail.Topic == topicName {
						createdTopics = append(createdTopics, topicDetail)
						break
					}
				}
			}

			assert.Equal(t, len(tt.topicNames), len(createdTopics), "Expected %d topics to be created but got %d", len(tt.topicNames), len(createdTopics))
			for _, createdTopic := range createdTopics {
				topicExistsAfter := false
				for _, topicName := range tt.topicNames {
					if createdTopic.Topic == topicName {
						topicExistsAfter = true
						break
					}
				}

				if topicExistsAfter {
					assert.Equal(t, tt.partitions, len(createdTopic.Partitions), "Expected %d partitions for topic %s but got %d", tt.partitions, createdTopic.Topic, len(createdTopic.Partitions))
				} else {
					assert.Fail(t, "Topic '%s' was created but it wasn't expected", createdTopic.Topic)
				}
			}

			if tt.options != nil {
				resourceConfigs, err := adminClient.DescribeTopicConfigs(ctx, tt.topicNames...)
				assert.NoError(t, err)
				for _, resourceConfig := range resourceConfigs {
					for _, topicName := range tt.topicNames {
						if resourceConfig.Name == topicName {
							for _, config := range resourceConfig.Configs {
								if _, ok := tt.options[config.Key]; ok {
									assert.Equal(t, tt.options[config.Key], *config.Value, "Expected config '%s' to be '%s' but got '%s'", config.Key, tt.options[config.Key], *config.Value)
								}
							}
						}
					}
				}
			}
		})
	}
}
