package get

import (
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete one or many resources",
	Long:  `Delete one or many resources`,
}

func init() {
	cmd.RootCmd.AddCommand(deleteCmd)
}
