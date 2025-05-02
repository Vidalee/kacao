package config

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestDeleteContext(t *testing.T) {
	// Define test configuration
	testConfig := TestConfig{
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
	}

	tests := []struct {
		name           string
		args           []string
		expectedError  bool
		expectedOutput string
		checkHelp      bool
	}{
		{
			name:           "successful context deletion",
			args:           []string{"config", "delete-context", "test-context"},
			expectedError:  false,
			expectedOutput: "",
		},
		{
			name:           "non-existent context",
			args:           []string{"config", "delete-context", "non-existent"},
			expectedError:  true,
			expectedOutput: "Error: context 'non-existent' does not exist in the configuration",
		},
		{
			name:           "no args shows help",
			args:           []string{"config", "delete-context"},
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

			// Execute the command
			cmd.RootCmd.SetArgs(tt.args)
			err := cmd.RootCmd.Execute()

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
					// Verify the context was deleted
					contextName := tt.args[2]
					contexts := viper.GetStringMap("contexts")
					_, exists := contexts[contextName]
					assert.False(t, exists)
				}
			}
		})
	}
}
