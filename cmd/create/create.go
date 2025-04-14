package get

import (
	"github.com/Vidalee/kacao/cmd"
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a resource",
	Long:  `Create a resource`,
}

func init() {
	cmd.RootCmd.AddCommand(createCmd)
}
