package main

import (
	"creditnet/core"
	"creditnet/hierarchy"
)

func main() {
	srv, err := core.Boot()
	if err != nil { panic(err) }
	srv.MustRegister("hierarchy", hierarchy.New, hierarchy.SyncMethods(), hierarchy.MsgMethods(), hierarchy.OpenMsgMethods(), hierarchy.AccountMethods())
	core.RunAndWait(srv)
}
