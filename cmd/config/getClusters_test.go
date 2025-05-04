package config

import (
	"github.com/Vidalee/kacao/test_helpers"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetClusters(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		testConfig     test_helpers.TestConfig
		expectedError  bool
		expectedOutput string
		checkHelp      bool
	}{
		{
			name: "list clusters",
			args: []string{"config", "get-clusters"},
			testConfig: test_helpers.TestConfig{
				Clusters: map[string]map[string]interface{}{
					"test-cluster": {
						"bootstrap-servers": []string{"localhost:9092"},
					},
				},
			},
			expectedError:  false,
			expectedOutput: "Clusters defined in the configuration:\n- test-cluster: [localhost:9092]\n",
		},
		{
			name: "multiple clusters",
			args: []string{"config", "get-clusters"},
			testConfig: test_helpers.TestConfig{
				Clusters: map[string]map[string]interface{}{
					"prod-cluster": {
						"bootstrap-servers": []string{"kafka-prod-1:9092", "kafka-prod-2:9092", "kafka-prod-3:9092"},
					},
					"staging-cluster": {
						"bootstrap-servers": []string{"kafka-staging-1:9092", "kafka-staging-2:9092"},
					},
					"dev-cluster": {
						"bootstrap-servers": []string{"kafka-dev:9092"},
					},
				},
			},
			expectedError: false,
			expectedOutput: `Clusters defined in the configuration:
- dev-cluster: [kafka-dev:9092]
- prod-cluster: [kafka-prod-1:9092 kafka-prod-2:9092 kafka-prod-3:9092]
- staging-cluster: [kafka-staging-1:9092 kafka-staging-2:9092]
`,
		},
		{
			name: "clusters set but empty",
			args: []string{"config", "get-clusters"},
			testConfig: test_helpers.TestConfig{
				Clusters: map[string]map[string]interface{}{},
			},
			expectedError:  false,
			expectedOutput: "No clusters defined in the configuration.\n",
		},
		{
			name:           "clusters not set",
			args:           []string{"config", "get-clusters"},
			testConfig:     test_helpers.TestConfig{},
			expectedError:  false,
			expectedOutput: "No clusters defined in the configuration.\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, tt.testConfig)
			defer test_helpers.CleanupTestConfig(t, tempDir)

			output, err := test_helpers.ExecuteCommandWrapper(tt.args)

			if tt.expectedError {
				assert.Error(t, err)
				firstLine := strings.Split(output, "\n")[0]
				assert.Equal(t, tt.expectedOutput, firstLine)
			} else {
				assert.NoError(t, err)
				if tt.checkHelp {
					assert.Contains(t, output, tt.expectedOutput)
				} else if strings.HasPrefix(tt.expectedOutput, "Clusters defined in the configuration:") {
					actualLines := strings.Split(strings.TrimSpace(output), "\n")
					expectedLines := strings.Split(strings.TrimSpace(tt.expectedOutput), "\n")
					assert.Equal(t, expectedLines[0], actualLines[0]) // header

					actualClusters := actualLines[1:]
					expectedClusters := expectedLines[1:]
					sort.Strings(actualClusters)
					sort.Strings(expectedClusters)
					assert.Equal(t, expectedClusters, actualClusters)
				} else {
					assert.Equal(t, tt.expectedOutput, output)
				}
			}
		})
	}
}
