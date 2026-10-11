package main

import (
	"creditnet/core"
	"creditnet/ripple"
)

func main() {
	srv, err := core.Boot()
	if err != nil { panic(err) }
	srv.MustRegister("ripple", ripple.Config())
	core.RunAndWait(srv)
}
