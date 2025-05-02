package config

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestDeleteCluster(t *testing.T) {
	// Define test configuration
	testConfig := TestConfig{
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
			tempDir := SetupTest(t, testConfig)
			defer CleanupTest(t, tempDir)

			// Create a buffer to capture output
			var buf bytes.Buffer
			cmd.RootCmd.SetOut(&buf)
			cmd.RootCmd.SetErr(&buf)

			// Print initial state
			clusters := viper.GetStringMap("clusters")
			fmt.Printf("Initial clusters: %v\n", clusters)

			// Execute the command
			cmd.RootCmd.SetArgs(tt.args)
			err := cmd.RootCmd.Execute()

			// Print state after command
			clusters = viper.GetStringMap("clusters")
			fmt.Printf("Clusters after command: %v\n", clusters)

			// Check the output
			output := buf.String()
			if tt.expectedError {
				assert.Error(t, err)
				// For error output, we check the first line
				firstLine := strings.Split(output, "\n")[0]
				assert.Equal(t, tt.expectedOutput, firstLine)
			} else {
				assert.NoError(t, err)
				if tt.checkHelp {
					// For help output, we just check that it contains the usage line
					assert.Contains(t, output, tt.expectedOutput)
				} else {
					// For success case, we expect no output
					assert.Empty(t, output)
					// Verify the cluster was deleted from both in-memory state and file
					clusterName := tt.args[2]

					// Check in-memory state
					clusters := viper.GetStringMap("clusters")
					_, exists := clusters[clusterName]
					assert.False(t, exists, "Cluster should not exist in in-memory state")

					// Check file state
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
