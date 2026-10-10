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

	srv.MustRegister("ripple", ripple.New, ripple.SyncMethods(), ripple.MsgMethods(), ripple.OpenMsgMethods(), ripple.AccountMethods())
	srv.MustRegister("resilience", resilience.New, resilience.SyncMethods(), resilience.MsgMethods(), resilience.OpenMsgMethods(), resilience.AccountMethods())
	srv.MustRegister("hierarchy", hierarchy.New, hierarchy.SyncMethods(), hierarchy.MsgMethods(), hierarchy.OpenMsgMethods(), hierarchy.AccountMethods())
	srv.MustRegister("loops", loops.New, loops.SyncMethods(), loops.MsgMethods(), nil, loops.AccountMethods())
	srv.MustRegister("triad", triad.New, triad.SyncMethods(), triad.MsgMethods(), nil, triad.AccountMethods())

	core.RunAndWait(srv)
}
