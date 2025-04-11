package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "kacao",
	Short: "Kafka CLI",
	Long:  `A CLI to manage and interact with Kafka`,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kacao.yaml)")
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
