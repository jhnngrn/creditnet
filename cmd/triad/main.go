package main

import (
	"log"
	"creditnet/core"
	"creditnet/modules/triad"
)

func main() {
	srv, err := core.Boot()
	if err != nil { log.Fatal(err) }
	srv.MustRegister("triad", triad.Config())
	core.RunAndWait(srv)
}
