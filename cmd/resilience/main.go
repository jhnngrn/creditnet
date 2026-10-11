package main

import (
	"creditnet/core"
	"creditnet/modules/resilience"
)

func main() {
	srv, err := core.Boot()
	if err != nil { panic(err) }
	srv.MustRegister("resilience", resilience.Config())
	core.RunAndWait(srv)
}
