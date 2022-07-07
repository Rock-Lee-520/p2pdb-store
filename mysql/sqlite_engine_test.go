// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlite

import (
	"testing"

	"github.com/Rock-liyi/p2pdb-store/sql"
	debug "github.com/favframework/debug"
)

// This file is for validating both the engine itself and the in-sqlite database implementation in the sqlite package.
// Any engine test that relies on the correct implementation of the in-sqlite database belongs here. All test logic and
// queries are declared in the exported enginetest package to make them usable by integrators, to validate the engine
// against their own implementation.

var numPartitionsVals = []int{
	1,
	//	testNumPartitions,
}

var parallelVals = []int{
	1,
	2,
}

func TestReadOnlyDatabasess(t *testing.T) {
	debug.Dump("TestReadOnlyDatabasess start====")
	var dbname = "test"
	connection, err := NewSQLITEHarness(dbname + "db")
	if err != nil {
		debug.Dump("into connection err")
		debug.Dump(err.Error())
	}

	//debug.Dump(connection)
	ctx := connection.NewContext()
	// if connection.shim.HasDatabase(dbname){
	// 	connection.shim.DropDatabase(ctx,dbname)
	// }

	err = connection.shim.CreateDatabase(ctx, dbname+"db")
	if err != nil {
		debug.Dump("into CreateDatabase err")
		debug.Dump(err.Error())
	}
	_, err = connection.shim.Query("", "select * from mytable3")
	if err != nil {
		debug.Dump("into Query err")
		debug.Dump(err.Error())
	}

	err = connection.shim.Exec("", "CREATE TABLE   IF NOT EXISTS `mytable3`  (`name` text NOT NULL,`email` text NOT NULL,`phone_numbers` json NOT NULL,`created_at` timestamp NOT NULL)")
	if err != nil {
		debug.Dump("into Exec err")
		debug.Dump(err.Error())
	}

	err = connection.shim.Exec("", "INSERT INTO mytable3(name, email, phone_numbers, created_at) VALUES('Evil Bob', 'evilbob@gmail.com', 123, '2022-01-02 12:28:26.024723000');")
	if err != nil {
		debug.Dump("into Exec err")
		debug.Dump(err.Error())
	}

	rows, err := connection.shim.QueryRows("", "select * from mytable3")
	if err != nil {
		debug.Dump("into Query err")
		debug.Dump(err.Error())
	}

	debug.Dump(rows)
	// ctx.SetCurrentDatabase("test")
	// db, err := connection.shim.Database("test")
	// query := connection.shim.Query(db, "select * from mytable")
	// // name, err := db.GetTableNames(connection.NewContext())

	//	debug.Dump(db.Name())
	//debug.Dump(db)
	// sql, err := connection.shim.Query("test", "select * from mytable;")

	// if err != nil {
	// 	debug.Dump(err)
	// }
	// debug.Dump(sql)
	debug.Dump("TestReadOnlyDatabasess end====")

}

func findTable(dbs []sql.Database, tableName string) (sql.Database, sql.Table) {
	for _, db := range dbs {
		names, err := db.GetTableNames(sql.NewEmptyContext())
		if err != nil {
			panic(err)
		}
		for _, name := range names {
			if name == tableName {
				table, _, _ := db.GetTableInsensitive(sql.NewEmptyContext(), name)
				return db, table
			}
		}
	}
	return nil, nil
}
