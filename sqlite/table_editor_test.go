// Copyright 2020-2021 P2PDB, Inc.
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

package sqlite_test

import (
	"context"
	"testing"

	"github.com/Rock-liyi/p2pdb-store/sql"
	"github.com/Rock-liyi/p2pdb-store/sqlite"
	_ "github.com/Rock-liyi/p2pdb/application/event/subscribe" //注册事件监听
)

func newPersistedSqlContext() *sql.Context {
	ctx, _ := context.WithCancel(context.TODO())
	sess := sql.NewBaseSession()
	persistedGlobals := sqlite.GlobalsMap{"max_connections": 1000, "net_read_timeout": 1000, "auto_increment_increment": 123}
	persistedSess := sqlite.NewInSqlitePersistedSession(sess, persistedGlobals)
	sqlCtx := sql.NewContext(ctx)
	sqlCtx.Session = persistedSess
	return sqlCtx
}

func TestTableUpdate(t *testing.T) {
	const (
		dbName    = "test"
		tableName = "userinfo"
	)

	db := sqlite.NewDatabase(dbName)

	ctx := newPersistedSqlContext()

	session := ctx.Session.(sql.PersistableSession)
	session.SetCurrentDatabase(db.Name())
	session.SetAddress(db.Address())
	session.SetConnection(db.Connection())

	table := sqlite.NewTable(tableName, sql.NewPrimaryKeySchema(sql.Schema{

		//	{Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "id", Type: sql.Int64, Nullable: false, Source: tableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
		// {Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
		// {Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
	}))

	table.Insert(ctx, sql.NewRow(2123, "john@doe.com"))
	//table.Updater(ctx, sql.NewRow(2123, "john@doe.com"))

	//db.DropTable(ctx, tableName)
}

func TestTableInsert(t *testing.T) {
	const (
		dbName    = "test"
		tableName = "userinfo"
	)

	db := sqlite.NewDatabase(dbName)

	ctx := newPersistedSqlContext()

	session := ctx.Session.(sql.PersistableSession)
	session.SetCurrentDatabase(db.Name())
	session.SetAddress(db.Address())
	session.SetConnection(db.Connection())

	table := sqlite.NewTable(tableName, sql.NewPrimaryKeySchema(sql.Schema{

		//	{Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "id", Type: sql.Int64, Nullable: false, Source: tableName},
		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
		// {Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
		// {Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
	}))

	table.Insert(ctx, sql.NewRow(2123, "john@doe.com"))
	db.DropTable(ctx, tableName)

}
