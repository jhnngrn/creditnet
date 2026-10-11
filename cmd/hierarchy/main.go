package main

import (
	"creditnet/core"
	"creditnet/modules/hierarchy"
)

func main() {
	srv, err := core.Boot()
	if err != nil { panic(err) }
	srv.MustRegister("hierarchy", hierarchy.Config())
	core.RunAndWait(srv)
}
