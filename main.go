package main

import (
	"github.com/Vidalee/kacao/cmd"
	_ "github.com/Vidalee/kacao/cmd/config"
	_ "github.com/Vidalee/kacao/cmd/create"
	_ "github.com/Vidalee/kacao/cmd/delete"
	_ "github.com/Vidalee/kacao/cmd/get"
)

func main() {
	cmd.Execute()
}
