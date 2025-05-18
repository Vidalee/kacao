package describe

import (
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
)

var describeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describe one or many resources",
	Long:  `Describe one or many resources`,
}

func init() {
	cmd.RootCmd.AddCommand(describeCmd)
}
