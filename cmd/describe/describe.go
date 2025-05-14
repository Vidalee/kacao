package describe

import (
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
)

var describeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Display one or many resources",
	Long:  `Display one or many resources`,
}

func init() {
	cmd.RootCmd.AddCommand(describeCmd)
}
