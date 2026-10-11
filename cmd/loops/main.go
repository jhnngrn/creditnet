package main

import (
	"creditnet/core"
	"creditnet/modules/loops"
)

func main() {
	srv, err := core.Boot()
	if err != nil { panic(err) }
	srv.MustRegister("loops", loops.Config())
	core.RunAndWait(srv)
}
