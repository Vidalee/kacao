package get

import (
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
)

var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Produce messages to a topic",
	Long:  `Produce messages to a topic`,
}

func init() {
	cmd.RootCmd.AddCommand(produceCmd)
}
