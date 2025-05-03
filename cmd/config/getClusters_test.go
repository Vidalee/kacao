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
			test_helpers.ResetSubCommandFlagValues(cmd.RootCmd)
			defer test_helpers.CleanupTest(t, tempDir)

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
