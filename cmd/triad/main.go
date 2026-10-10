package main

import (
	"log"
	"creditnet/core"
	"creditnet/triad"
)

func main() {
	srv, err := core.Boot()
	if err != nil { log.Fatal(err) }
	srv.MustRegister("triad", triad.New, triad.SyncMethods(), triad.MsgMethods(), nil, triad.AccountMethods())
	core.RunAndWait(srv)
}
