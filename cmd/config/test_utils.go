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

type TestConfig struct {
	Clusters       map[string]map[string]interface{}
	Contexts       map[string]map[string]interface{}
	CurrentContext string
}

func SetupTest(t *testing.T, config TestConfig) string {
	tempDir, err := os.MkdirTemp("", "kacao-test")
	assert.NoError(t, err)

	viper.Reset()
	viper.SetConfigType("yaml")
	viper.AddConfigPath(tempDir)
	viper.SetConfigName(".kacao")

	for clusterName, clusterConfig := range config.Clusters {
		for key, value := range clusterConfig {
			viper.Set("clusters."+clusterName+"."+key, value)
		}
	}
	if config.Clusters != nil && len(config.Clusters) == 0 {
		viper.Set("clusters", map[string]interface{}{})
	}

	for contextName, contextConfig := range config.Contexts {
		for key, value := range contextConfig {
			viper.Set("contexts."+contextName+"."+key, value)
		}
	}
	if config.Contexts != nil && len(config.Contexts) == 0 {
		viper.Set("contexts", map[string]interface{}{})
	}

	err = viper.SafeWriteConfig()
	assert.NoError(t, err)

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
				//if f.Value.Type() == "stringSlice" {
				//	resetValue = ""
				//}
				_ = f.Value.Set(resetValue)
				//if err != nil {
				//	return
				//}
				f.Changed = false
			}
		})
		resetSubCommandFlagValues(c)
	}
}

func CleanupTest(t *testing.T, tempDir string) {
	viper.Reset()
	err := os.RemoveAll(tempDir)
	assert.NoError(t, err)
}
