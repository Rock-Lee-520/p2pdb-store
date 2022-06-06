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
	"testing"

	debug "github.com/favframework/debug"
	"github.com/kkguan/p2pdb-store/sql"
	"github.com/kkguan/p2pdb-store/sqlite"
	"github.com/stretchr/testify/require"
)

func TestDatabase_Name(t *testing.T) {
	require := require.New(t)
	db := sqlite.NewDatabase("test")
	debug.Dump(db.Name())
	require.Equal("test", db.Name())
}

func TestDatabase_AddTable(t *testing.T) {
	require := require.New(t)
	db := sqlite.NewDatabase("test")

	//db.Tables()
	//tables := db.Tables()
	//require.Equal(0, len(tables))
	s := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	})

	err := db.CreateTable(sql.NewEmptyContext(), "test_table", s)
	require.NoError(err)

	tables := db.Tables()
	//require.Equal(1, len(tables))
	tt, ok := tables["test_table"]
	require.True(ok)
	require.NotNil(tt)

	err = db.CreateTable(sql.NewEmptyContext(), "test_table", s)
	require.Error(err)
}

func TestDatabase_DropTable(t *testing.T) {
	require := require.New(t)
	db := sqlite.NewDatabase("test")
	// tables := db.Tables()
	// require.Equal(0, len(tables))

	s := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	})

	err := db.CreateTable(sql.NewEmptyContext(), "userinfo", s)
	require.NoError(err)

	tables := db.Tables()
	//require.Equal(1, len(tables))
	tt, ok := tables["userinfo"]
	require.True(ok)
	require.NotNil(tt)

	err = db.DropTable(sql.NewEmptyContext(), "userinfo")
	if err != nil {
		require.Error(err)
	}
}
