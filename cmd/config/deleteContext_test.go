package config

import (
	"bytes"
	"github.com/Vidalee/kacao/test_helpers"
	"strings"
	"testing"

	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestDeleteContext(t *testing.T) {
	testConfig := test_helpers.TestConfig{
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
			tempDir := test_helpers.SetupTest(t, testConfig)
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
				} else {
					assert.Empty(t, output)
					contextName := tt.args[2]
					contexts := viper.GetStringMap("contexts")
					_, exists := contexts[contextName]
					assert.False(t, exists)
				}
			}
		})
	}
}
