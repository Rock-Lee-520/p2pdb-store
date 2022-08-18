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

package memory_test

import (
	"testing"

	"github.com/dolthub/vitess/go/vt/log"
	"github.com/stretchr/testify/require"

	"github.com/Rock-liyi/p2pdb-store/memory"
	"github.com/Rock-liyi/p2pdb-store/sql"
)

func TestDatabase_Name(t *testing.T) {
	require := require.New(t)
	db := memory.NewDatabase("test")
	log.Warning("123")
	require.Equal("test", db.Name())
}

func TestDatabase_AddTable(t *testing.T) {
	require := require.New(t)
	db := memory.NewDatabase("test")
	tables := db.Tables()
	require.Equal(0, len(tables))

	err := db.CreateTable(sql.NewEmptyContext(), "test_table", sql.PrimaryKeySchema{})
	require.NoError(err)

	tables = db.Tables()
	require.Equal(1, len(tables))
	tt, ok := tables["test_table"]
	require.True(ok)
	require.NotNil(tt)

	err = db.CreateTable(sql.NewEmptyContext(), "test_table", sql.PrimaryKeySchema{})
	require.Error(err)
}
