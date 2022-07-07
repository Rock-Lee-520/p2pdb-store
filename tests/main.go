package main

import (
	"github.com/Rock-liyi/p2pdb-store/sqlite"
	debug "github.com/favframework/debug"
)

func main() {
	db := sqlite.NewDatabase("test")
	debug.Dump(db.Name())

}
