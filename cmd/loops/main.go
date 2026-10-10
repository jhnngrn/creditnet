package main

import (
	"creditnet/core"
	"creditnet/loops"
)

func main() {
	srv, err := core.Boot()
	if err != nil { panic(err) }
	srv.MustRegister("loops", loops.New, loops.SyncMethods(), loops.MsgMethods(), nil, loops.AccountMethods())
	core.RunAndWait(srv)
}
