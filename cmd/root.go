package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var DefaultConsumerGroup = "kacao-cli"

var cfgFile string

var RootCmd = &cobra.Command{
	Use:   "kacao",
	Short: "Kafka CLI",
	Long:  `A CLI to manage and interact with Kafka`,
}

func Execute() {
	err := RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kacao.yaml)")
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".kacao")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// Try to read the config file
	if err := viper.ReadInConfig(); err != nil {
		// If the config file doesn't exist, create it with default structure
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if errors.As(err, &configFileNotFoundError) {
			home, err := os.UserHomeDir()
			cobra.CheckErr(err)

			configPath := filepath.Join(home, ".kacao")
			if err := viper.WriteConfigAs(configPath); err != nil {
				_, err := fmt.Fprintln(os.Stderr, "Error creating default config file:", err)
				cobra.CheckErr(err)
				return
			}
			_, err = fmt.Fprintln(os.Stderr, "Created new config file at:", configPath)
			cobra.CheckErr(err)
		}
	}
}

func GetCurrentClusterBootstrapServers() ([]string, error) {
	contexts := viper.GetStringMap("contexts")
	if len(contexts) == 0 {
		return []string{}, errors.New("no contexts set. Use 'kacao config set-context NAME' to set a context")
	}

	if len(contexts) == 1 {
		for contextName := range contexts {
			viper.Set("current-context", contextName)
			break
		}
	}

	currentContext := viper.GetString("current-context")
	if currentContext == "" {
		return []string{}, errors.New("no context set. Use 'kacao config use-context NAME' to set a context")
	}

	clusterName := viper.GetString("contexts." + currentContext + ".cluster")
	if clusterName == "" {
		return []string{}, fmt.Errorf("context '%s' has no cluster set", currentContext)
	}

	bootstrapServers := viper.GetStringSlice("clusters." + clusterName + ".bootstrap-servers")
	if len(bootstrapServers) == 0 {
		return []string{}, fmt.Errorf("no bootstrap servers set for cluster '%s'", clusterName)
	}
	return bootstrapServers, nil
}

func GetConsumerGroup() (string, error) {
	contexts := viper.GetStringMap("contexts")
	if len(contexts) == 0 {
		return "", errors.New("no contexts set. Use 'kacao config set-context NAME' to set a context")
	}

	if len(contexts) == 1 {
		for contextName := range contexts {
			viper.Set("current-context", contextName)
			break
		}
	}

	currentContext := viper.GetString("current-context")
	if currentContext == "" {
		return "", errors.New("no context set. Use 'kacao config use-context NAME' to set a context")
	}

	consumerGroup := viper.GetString("contexts." + currentContext + ".consumer-group")

	if consumerGroup == "" {
		viper.Set("contexts."+currentContext+".consumer-group", DefaultConsumerGroup)
		consumerGroup = DefaultConsumerGroup
		err := viper.WriteConfig()
		if err != nil {
			if os.IsNotExist(err) {
				err := viper.SafeWriteConfig()
				cobra.CheckErr(err)
			} else {
				cobra.CheckErr(err)
			}
		}
		return consumerGroup, nil
	}
	return consumerGroup, nil
}
