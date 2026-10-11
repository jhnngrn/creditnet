package main

import (
	"creditnet/core"
	"creditnet/modules/hierarchy"
	"creditnet/modules/loops"
	"creditnet/modules/resilience"
	"creditnet/modules/ripple"
	"creditnet/modules/triad"
)

func main() {
	srv, err := core.Boot()
	if err != nil { panic(err) }

	srv.MustRegister("ripple", ripple.Config())
	srv.MustRegister("resilience", resilience.Config())
	srv.MustRegister("hierarchy", hierarchy.Config())
	srv.MustRegister("loops", loops.Config())
	srv.MustRegister("triad", triad.Config())

	core.RunAndWait(srv)
}
