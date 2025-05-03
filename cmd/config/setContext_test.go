package config

import (
	"bytes"
	"testing"

	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestSetContext(t *testing.T) {
	testConfig := TestConfig{
		Clusters: map[string]map[string]interface{}{
			"test-cluster": {
				"bootstrap-servers": []string{"localhost:9092"},
			},
			"prod-cluster": {
				"bootstrap-servers": []string{"prod-kafka:9092", "prod-kafka2:9092"},
			},
		},
		Contexts: map[string]map[string]interface{}{
			"existing-context": {
				"cluster":        "test-cluster",
				"consumer-group": "existing-group",
			},
		},
	}

	tests := []struct {
		name            string
		args            []string
		expectedError   bool
		expectedOutput  string
		checkHelp       bool
		checkConfig     bool
		expectedCluster string
		expectedGroup   string
		checkClusterSet bool
		checkGroupSet   bool
	}{
		{
			name:            "successful context creation with cluster",
			args:            []string{"config", "set-context", "test-context", "--cluster", "test-cluster"},
			expectedError:   false,
			expectedOutput:  "Defined context 'test-context'\n",
			checkConfig:     true,
			expectedCluster: "test-cluster",
			checkClusterSet: true,
			checkGroupSet:   false,
		},
		{
			name:            "successful context creation with consumer group only",
			args:            []string{"config", "set-context", "group-context", "--consumer-group", "new-group"},
			expectedError:   false,
			expectedOutput:  "Defined context 'group-context'\n",
			checkConfig:     true,
			expectedGroup:   "new-group",
			checkClusterSet: false,
			checkGroupSet:   true,
		},
		{
			name:            "successful context creation with all options",
			args:            []string{"config", "set-context", "full-context", "--cluster", "prod-cluster", "--consumer-group", "prod-group"},
			expectedError:   false,
			expectedOutput:  "Defined context 'full-context'\n",
			checkConfig:     true,
			expectedCluster: "prod-cluster",
			expectedGroup:   "prod-group",
			checkClusterSet: true,
			checkGroupSet:   true,
		},
		{
			name:            "update existing context",
			args:            []string{"config", "set-context", "existing-context", "--consumer-group", "updated-group"},
			expectedError:   false,
			expectedOutput:  "Defined context 'existing-context'\n",
			checkConfig:     true,
			expectedCluster: "test-cluster",
			expectedGroup:   "updated-group",
			checkClusterSet: true,
			checkGroupSet:   true,
		},
		{
			name:           "non-existent cluster",
			args:           []string{"config", "set-context", "test-context", "--cluster", "non-existent"},
			expectedError:  true,
			expectedOutput: "Error: cluster 'non-existent' does not exist in the configuration",
		},
		{
			name:           "no options provided",
			args:           []string{"config", "set-context", "test-context"},
			expectedError:  true,
			expectedOutput: "Error: at least one of --cluster or --consumer-group must be specified",
		},
		{
			name:           "no args shows help",
			args:           []string{"config", "set-context"},
			expectedError:  false,
			expectedOutput: "Usage:",
			checkHelp:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := SetupTest(t, testConfig)
			defer CleanupTest(t, tempDir)

			var buf bytes.Buffer
			cmd.RootCmd.SetOut(&buf)
			cmd.RootCmd.SetErr(&buf)

			cmd.RootCmd.SetArgs(tt.args)
			err := cmd.RootCmd.Execute()

			output := buf.String()
			if tt.expectedError {
				assert.Error(t, err)
				assert.Contains(t, output, tt.expectedOutput)
			} else {
				assert.NoError(t, err)
				if tt.checkHelp {
					assert.Contains(t, output, tt.expectedOutput)
				} else {
					assert.Equal(t, tt.expectedOutput, output)

					if tt.checkConfig {
						contextName := tt.args[2]
						assert.True(t, viper.IsSet("contexts."+contextName))

						if tt.checkClusterSet {
							cluster := viper.GetString("contexts." + contextName + ".cluster")
							assert.Equal(t, tt.expectedCluster, cluster)
						}

						if tt.checkGroupSet {
							consumerGroup := viper.GetString("contexts." + contextName + ".consumer-group")
							assert.Equal(t, tt.expectedGroup, consumerGroup)
						}
					}
				}
			}
		})
	}
}

func TestSetContextWithoutExistingConfig(t *testing.T) {
	testConfig := TestConfig{
		Clusters: map[string]map[string]interface{}{
			"test-cluster": {
				"bootstrap-servers": []string{"localhost:9092"},
			},
		},
	}

	tempDir := SetupTest(t, testConfig)
	defer CleanupTest(t, tempDir)

	var buf bytes.Buffer
	cmd.RootCmd.SetOut(&buf)
	cmd.RootCmd.SetErr(&buf)

	cmd.RootCmd.SetArgs([]string{"config", "set-context", "first-context", "--cluster", "test-cluster", "--consumer-group", "first-group"})
	err := cmd.RootCmd.Execute()

	assert.NoError(t, err)
	assert.Equal(t, "Defined context 'first-context'\n", buf.String())

	assert.True(t, viper.IsSet("contexts.first-context"))
	assert.Equal(t, "test-cluster", viper.GetString("contexts.first-context.cluster"))
	assert.Equal(t, "first-group", viper.GetString("contexts.first-context.consumer-group"))
}
