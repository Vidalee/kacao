package config

import (
	"github.com/Vidalee/kacao/test_helpers"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestUseContext(t *testing.T) {
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
			name:           "successful context switch",
			args:           []string{"config", "use-context", "test-context"},
			expectedError:  false,
			expectedOutput: "",
		},
		{
			name:           "non-existent context",
			args:           []string{"config", "use-context", "non-existent"},
			expectedError:  true,
			expectedOutput: "Error: context 'non-existent' does not exist in the configuration",
		},
		{
			name:           "no args shows help",
			args:           []string{"config", "use-context"},
			expectedError:  false,
			expectedOutput: "Usage:",
			checkHelp:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := test_helpers.SetupTest(t, testConfig)
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
				} else {
					assert.Empty(t, output)
					currentContext := viper.GetString("current-context")
					assert.Equal(t, "test-context", currentContext)
				}
			}
		})
	}
}
