package config

import (
	"bytes"
	"github.com/Vidalee/kacao/test_helpers"
	"sort"
	"strings"
	"testing"

	"github.com/Vidalee/kacao/cmd"
	"github.com/stretchr/testify/assert"
)

func TestGetContexts(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		testConfig     test_helpers.TestConfig
		expectedError  bool
		expectedOutput string
		checkHelp      bool
	}{
		{
			name: "list contexts",
			args: []string{"config", "get-contexts"},
			testConfig: test_helpers.TestConfig{
				Clusters: map[string]map[string]interface{}{
					"test-cluster": {
						"bootstrap-servers": []string{"localhost:9092"},
					},
				},
				Contexts: map[string]map[string]interface{}{
					"test-context": {
						"cluster":        "test-cluster",
						"consumer-group": "test-group",
					},
				},
			},
			expectedError:  false,
			expectedOutput: "Contexts defined in the configuration:\n- test-context: consumer-group=test-group cluster=test-cluster\n",
		},
		{
			name: "multiple contexts",
			args: []string{"config", "get-contexts"},
			testConfig: test_helpers.TestConfig{
				Clusters: map[string]map[string]interface{}{
					"prod-cluster": {
						"bootstrap-servers": []string{"kafka-prod-1:9092", "kafka-prod-2:9092"},
					},
					"staging-cluster": {
						"bootstrap-servers": []string{"kafka-staging:9092"},
					},
				},
				Contexts: map[string]map[string]interface{}{
					"prod-orders": {
						"cluster":        "prod-cluster",
						"consumer-group": "orders-service",
					},
					"prod-payments": {
						"cluster":        "prod-cluster",
						"consumer-group": "payments-service",
					},
					"staging-orders": {
						"cluster":        "staging-cluster",
						"consumer-group": "orders-service-staging",
					},
				},
			},
			expectedError: false,
			expectedOutput: `Contexts defined in the configuration:
- prod-orders: consumer-group=orders-service cluster=prod-cluster
- prod-payments: consumer-group=payments-service cluster=prod-cluster
- staging-orders: consumer-group=orders-service-staging cluster=staging-cluster
`,
		},
		{
			name: "contexts set but empty",
			args: []string{"config", "get-contexts"},
			testConfig: test_helpers.TestConfig{
				Contexts: map[string]map[string]interface{}{},
			},
			expectedError:  false,
			expectedOutput: "No contexts defined in the configuration.\n",
		},
		{
			name:           "contexts not set",
			args:           []string{"config", "get-contexts"},
			testConfig:     test_helpers.TestConfig{},
			expectedError:  false,
			expectedOutput: "No contexts defined in the configuration.\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, tt.testConfig)
			defer test_helpers.CleanupTestConfig(t, tempDir)

			var buf bytes.Buffer
			cmd.RootCmd.SetOut(&buf)
			cmd.RootCmd.SetErr(&buf)

			cmd.RootCmd.SetArgs(tt.args)
			err := cmd.RootCmd.Execute()

			output := buf.String()
			if tt.expectedError {
				assert.Error(t, err)
				firstLine := strings.Split(output, "\n")[0]
				assert.Equal(t, tt.expectedOutput, firstLine)
			} else {
				assert.NoError(t, err)
				if tt.checkHelp {
					assert.Contains(t, output, tt.expectedOutput)
				} else if strings.HasPrefix(tt.expectedOutput, "Contexts defined in the configuration:") {
					actualLines := strings.Split(strings.TrimSpace(output), "\n")
					expectedLines := strings.Split(strings.TrimSpace(tt.expectedOutput), "\n")
					assert.Equal(t, expectedLines[0], actualLines[0]) // header

					actualContexts := actualLines[1:]
					expectedContexts := expectedLines[1:]
					sort.Strings(actualContexts)
					sort.Strings(expectedContexts)
					assert.Equal(t, expectedContexts, actualContexts)
				} else {
					assert.Equal(t, tt.expectedOutput, output)
				}
			}
		})
	}
}
