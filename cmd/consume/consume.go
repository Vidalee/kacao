package get

import (
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
)

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages from a topic",
	Long:  `Consume messages from a topic`,
}

func init() {
	cmd.RootCmd.AddCommand(consumeCmd)
}
