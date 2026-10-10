package main

import (
	"creditnet/core"
	"creditnet/resilience"
)

func main() {
	srv, err := core.Boot()
	if err != nil { panic(err) }
	srv.MustRegister("resilience", resilience.New, resilience.SyncMethods(), resilience.MsgMethods(), resilience.OpenMsgMethods(), resilience.AccountMethods())
	core.RunAndWait(srv)
}
