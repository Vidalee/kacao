package config

import (
	"bytes"
	"fmt"
	"github.com/Vidalee/kacao/test_helpers"
	"strings"
	"testing"

	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestDeleteCluster(t *testing.T) {
	testConfig := test_helpers.TestConfig{
		Clusters: map[string]map[string]interface{}{
			"test-cluster": {
				"bootstrap-servers": []string{"localhost:9092"},
			},
		},
	}

	tests := []struct {
		name           string
		args           []string
		expectedError  bool
		expectedOutput string
		checkHelp      bool
	}{
		{
			name:           "successful cluster deletion",
			args:           []string{"config", "delete-cluster", "test-cluster"},
			expectedError:  false,
			expectedOutput: "",
		},
		{
			name:           "non-existent cluster",
			args:           []string{"config", "delete-cluster", "non-existent"},
			expectedError:  true,
			expectedOutput: "Error: cluster 'non-existent' does not exist in the configuration",
		},
		{
			name:           "no args shows help",
			args:           []string{"config", "delete-cluster"},
			expectedError:  false,
			expectedOutput: "Usage:",
			checkHelp:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, testConfig)
			test_helpers.ResetSubCommandFlagValues(cmd.RootCmd)
			defer test_helpers.CleanupTest(t, tempDir)

			var buf bytes.Buffer
			cmd.RootCmd.SetOut(&buf)
			cmd.RootCmd.SetErr(&buf)

			clusters := viper.GetStringMap("clusters")
			fmt.Printf("Initial clusters: %v\n", clusters)

			cmd.RootCmd.SetArgs(tt.args)
			err := cmd.RootCmd.Execute()

			clusters = viper.GetStringMap("clusters")

			output := buf.String()
			if tt.expectedError {
				assert.Error(t, err)
				firstLine := strings.Split(output, "\n")[0]
				assert.Equal(t, tt.expectedOutput, firstLine)
			} else {
				assert.NoError(t, err)
				if tt.checkHelp {
					assert.Contains(t, output, tt.expectedOutput)
				} else {
					assert.Empty(t, output)
					clusterName := tt.args[2]

					clusters := viper.GetStringMap("clusters")
					_, exists := clusters[clusterName]
					assert.False(t, exists, "Cluster should not exist in in-memory state")

					err := viper.ReadInConfig()
					assert.NoError(t, err)
					clusters = viper.GetStringMap("clusters")
					_, exists = clusters[clusterName]
					assert.False(t, exists, "Cluster should not exist in file state")
				}
			}
		})
	}
}
