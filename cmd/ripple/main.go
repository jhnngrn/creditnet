package main

import (
	"creditnet/core"
	"creditnet/ripple"
)

func main() {
	srv, err := core.Boot()
	if err != nil { panic(err) }
	srv.MustRegister("ripple", ripple.New, ripple.SyncMethods(), ripple.MsgMethods(), ripple.OpenMsgMethods(), ripple.AccountMethods())
	core.RunAndWait(srv)
}
