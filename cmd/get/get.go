package get

import (
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Display one or many resources",
	Long:  `Display one or many resources`,
}

func init() {
	cmd.RootCmd.AddCommand(getCmd)
}
