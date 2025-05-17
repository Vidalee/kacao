package get

import (
	"context"
	"fmt"
	"github.com/Vidalee/kacao/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestGetBrokers(t *testing.T) {
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

	tests := []struct {
		name             string
		bootstrapServers []string
		args             []string
		expectedError    bool
		expectedPatterns []*regexp.Regexp
	}{
		{
			name:             "list brokers",
			bootstrapServers: brokers,
			args:             []string{"get", "brokers"},
			expectedError:    false,
			expectedPatterns: []*regexp.Regexp{
				regexp.MustCompile(`Broker ID\s+Host\s+Port\s+Rack\s+`),
				regexp.MustCompile(`1\s+localhost\s+\d+\s+<nil>\s+`),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testConfig := test_helpers.TestConfig{
				Clusters: map[string]map[string]interface{}{
					"test-cluster": {
						"bootstrap-servers": tt.bootstrapServers,
					},
				},
				Contexts: map[string]map[string]interface{}{
					"test-context": {
						"cluster":        "test-cluster",
						"consumer-group": "test-group",
					},
				},
			}

			tempDir := test_helpers.SetupTest(t, testConfig)
			defer test_helpers.CleanupTestConfig(t, tempDir)

			output, err := test_helpers.ExecuteCommandWrapper(tt.args)

			if tt.expectedError {
				assert.Error(t, err, "Expected error when connecting to invalid broker")
				return
			}

			assert.NoError(t, err)

			// Check each expected regex pattern
			for _, pattern := range tt.expectedPatterns {
				assert.True(t, pattern.MatchString(output),
					"Expected output to match pattern: %s\nGot: %s",
					pattern.String(), output)
			}

			// Parse the first broker from the container to perform specific checks
			brokerParts := strings.Split(tt.bootstrapServers[0], ":")
			assert.Equal(t, 2, len(brokerParts), "Expected broker address in host:port format")

			host := brokerParts[0]
			port, err := strconv.Atoi(brokerParts[1])
			assert.NoError(t, err, "Failed to parse port from broker address")

			// Create a specific pattern for the broker with exact host and port
			specificBrokerPattern := regexp.MustCompile(fmt.Sprintf(`1\s+%s\s+%d\s+<nil>\s+`,
				regexp.QuoteMeta(host), port))

			assert.True(t, specificBrokerPattern.MatchString(output),
				"Expected to find broker with host %s and port %d in output",
				host, port)
		})
	}
}
