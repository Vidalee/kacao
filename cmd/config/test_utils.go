package config

import (
	"github.com/spf13/pflag"
	"os"
	"testing"

	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// TestConfig holds the configuration for a test
type TestConfig struct {
	Clusters       map[string]map[string]interface{}
	Contexts       map[string]map[string]interface{}
	CurrentContext string
}

// SetupTest creates a temporary directory and initializes the test environment with the given configuration
func SetupTest(t *testing.T, config TestConfig) string {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "kacao-test")
	assert.NoError(t, err)

	// Set up viper to use the temporary directory
	viper.Reset()
	viper.SetConfigType("yaml")
	viper.AddConfigPath(tempDir)
	viper.SetConfigName(".kacao")

	// Set up clusters
	for clusterName, clusterConfig := range config.Clusters {
		for key, value := range clusterConfig {
			viper.Set("clusters."+clusterName+"."+key, value)
		}
	}
	//set empty map if map exists but is empty
	if config.Clusters != nil && len(config.Clusters) == 0 {
		viper.Set("clusters", map[string]interface{}{})
	}

	// Set up contexts
	for contextName, contextConfig := range config.Contexts {
		for key, value := range contextConfig {
			viper.Set("contexts."+contextName+"."+key, value)
		}
	}
	//set empty map if map exists but is empty
	if config.Contexts != nil && len(config.Contexts) == 0 {
		viper.Set("contexts", map[string]interface{}{})
	}

	// Set current context if specified
	if config.CurrentContext != "" {
		viper.Set("current-context", config.CurrentContext)
	}

	// Write the config file
	err = viper.SafeWriteConfig()
	assert.NoError(t, err)

	// Reset the command structure
	cmd.RootCmd = &cobra.Command{
		Use:   "kacao",
		Short: "Kafka CLI",
		Long:  `A CLI to manage and interact with Kafka`,
	}
	cmd.RootCmd.AddCommand(configCmd)

	resetSubCommandFlagValues(cmd.RootCmd)

	return tempDir
}
func resetSubCommandFlagValues(root *cobra.Command) {
	for _, c := range root.Commands() {
		c.Flags().VisitAll(func(f *pflag.Flag) {
			if f.Changed {
				resetValue := f.DefValue
				if f.Value.Type() == "stringSlice" {
					resetValue = ""
				}
				err := f.Value.Set(resetValue)
				if err != nil {
					return
				}
				f.Changed = false
			}
		})
		resetSubCommandFlagValues(c)
	}
}

// CleanupTest removes the temporary directory and cleans up the test environment
func CleanupTest(t *testing.T, tempDir string) {
	// Clear Viper's in-memory cache
	viper.Reset()

	// Remove the temporary directory
	err := os.RemoveAll(tempDir)
	assert.NoError(t, err)
}
