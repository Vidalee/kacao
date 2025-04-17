package main

import (
	"github.com/Vidalee/kacao/cmd"
	_ "github.com/Vidalee/kacao/cmd/config"
	_ "github.com/Vidalee/kacao/cmd/consume"
	_ "github.com/Vidalee/kacao/cmd/create"
	_ "github.com/Vidalee/kacao/cmd/delete"
	_ "github.com/Vidalee/kacao/cmd/describe"
	_ "github.com/Vidalee/kacao/cmd/get"
	_ "github.com/Vidalee/kacao/cmd/produce"
)

func main() {
	cmd.Execute()
}
