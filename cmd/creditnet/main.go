package main

import (
	"creditnet/core"
	"creditnet/hierarchy"
	"creditnet/loops"
	"creditnet/resilience"
	"creditnet/ripple"
	"creditnet/triad"
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
