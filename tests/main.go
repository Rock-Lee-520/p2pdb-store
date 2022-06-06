package main

import (
	debug "github.com/favframework/debug"
	"github.com/kkguan/p2pdb-store/sqlite"
)

func main() {
	db := sqlite.NewDatabase("test")
	debug.Dump(db.Name())

}
