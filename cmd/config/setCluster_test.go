package config

import (
	"github.com/Vidalee/kacao/test_helpers"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestSetCluster(t *testing.T) {
	tests := []struct {
		name                string
		args                []string
		testConfig          test_helpers.TestConfig
		expectedError       bool
		expectedOutput      string
		checkOutputContains bool
		verifyConfig        func(t *testing.T)
	}{
		{
			name:           "successful cluster creation",
			args:           []string{"config", "set-cluster", "test-cluster", "--bootstrap-servers", "localhost:9092"},
			testConfig:     test_helpers.TestConfig{},
			expectedError:  false,
			expectedOutput: "Setting up cluster 'test-cluster' with bootstrap servers: [localhost:9092]\n",
			verifyConfig: func(t *testing.T) {
				assert.True(t, viper.IsSet("clusters.test-cluster"))
				bootstrapServers := viper.GetStringSlice("clusters.test-cluster.bootstrap-servers")
				assert.Equal(t, []string{"localhost:9092"}, bootstrapServers)
			},
		},
		{
			name:           "multiple bootstrap servers",
			args:           []string{"config", "set-cluster", "prod-cluster", "--bootstrap-servers", "kafka1:9092,kafka2:9092,kafka3:9092"},
			testConfig:     test_helpers.TestConfig{},
			expectedError:  false,
			expectedOutput: "Setting up cluster 'prod-cluster' with bootstrap servers: [kafka1:9092 kafka2:9092 kafka3:9092]\n",
			verifyConfig: func(t *testing.T) {
				assert.True(t, viper.IsSet("clusters.prod-cluster"))
				bootstrapServers := viper.GetStringSlice("clusters.prod-cluster.bootstrap-servers")
				assert.Equal(t, []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"}, bootstrapServers)
			},
		},
		{
			name:           "invalid cluster name - starts with number",
			args:           []string{"config", "set-cluster", "1invalid", "--bootstrap-servers", "localhost:9092"},
			testConfig:     test_helpers.TestConfig{},
			expectedError:  true,
			expectedOutput: "Error: cluster name can only contain alphanumerical characters, hyphens, and underscores, and must start with a letter",
		},
		{
			name:           "invalid cluster name - special characters",
			args:           []string{"config", "set-cluster", "invalid@name", "--bootstrap-servers", "localhost:9092"},
			testConfig:     test_helpers.TestConfig{},
			expectedError:  true,
			expectedOutput: "Error: cluster name can only contain alphanumerical characters, hyphens, and underscores, and must start with a letter",
		},
		{
			name: "update existing cluster",
			args: []string{"config", "set-cluster", "existing-cluster", "--bootstrap-servers", "new-server:9092"},
			testConfig: test_helpers.TestConfig{
				Clusters: map[string]map[string]interface{}{
					"existing-cluster": {
						"bootstrap-servers": []string{"old-server:9092"},
					},
				},
			},
			expectedError:  false,
			expectedOutput: "Setting up cluster 'existing-cluster' with bootstrap servers: [new-server:9092]\n",
			verifyConfig: func(t *testing.T) {
				assert.True(t, viper.IsSet("clusters.existing-cluster"))
				bootstrapServers := viper.GetStringSlice("clusters.existing-cluster.bootstrap-servers")
				assert.Equal(t, []string{"new-server:9092"}, bootstrapServers)
			},
		},
		{
			name:                "no args shows help",
			args:                []string{"config", "set-cluster"},
			testConfig:          test_helpers.TestConfig{},
			expectedError:       true,
			expectedOutput:      "Error: required flag(s) \"bootstrap-servers\" not set",
			checkOutputContains: true,
		},
		{
			name:           "missing bootstrap servers",
			args:           []string{"config", "set-cluster", "test-cluster"},
			testConfig:     test_helpers.TestConfig{},
			expectedError:  true,
			expectedOutput: "Error: required flag(s) \"bootstrap-servers\" not set",
		},
		{
			name:                "empty bootstrap servers",
			args:                []string{"config", "set-cluster", "test-cluster", "--bootstrap-servers", ""},
			testConfig:          test_helpers.TestConfig{},
			expectedError:       true,
			expectedOutput:      "Error: bootstrap-servers flag cannot be empty",
			checkOutputContains: true,
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
				if tt.checkOutputContains {
					assert.Contains(t, output, tt.expectedOutput)
				} else {
					assert.Equal(t, tt.expectedOutput, output)
					if tt.verifyConfig != nil {
						tt.verifyConfig(t)
					}
				}
			}
		})
	}
}
