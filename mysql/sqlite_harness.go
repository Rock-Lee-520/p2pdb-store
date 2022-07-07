// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"strings"

	"github.com/Rock-liyi/p2pdb-store/sql"
	debug "github.com/favframework/debug"
)

// SQLITEHarness is a harness for a local SQLITE server. This will modify databases and tables as the tests see fit, which
// may delete pre-existing data. Ensure that the SQLITE instance may freely be modified without worry.
type SQLITEHarness struct {
	shim           *SQLITEShim
	skippedQueries map[string]struct{}
}

// SQLITEDatabase represents a database for a local SQLITE server.
type SQLITEDatabase struct {
	harness *SQLITEHarness
	dbName  string
}

// SQLITETable represents a table for a local SQLITE server.
type SQLITETable struct {
	harness   *SQLITEHarness
	tableName string
}

var _ Harness = (*SQLITEHarness)(nil)
var _ SkippingHarness = (*SQLITEHarness)(nil)
var _ IndexHarness = (*SQLITEHarness)(nil)
var _ ForeignKeyHarness = (*SQLITEHarness)(nil)
var _ KeylessTableHarness = (*SQLITEHarness)(nil)

// NewSQLITEHarness returns a new SQLITEHarness.
func NewSQLITEHarness(dbname string) (*SQLITEHarness, error) {
	shim, err := NewSQLITEShim(dbname)
	if err != nil {
		return nil, err
	}
	return &SQLITEHarness{shim, make(map[string]struct{})}, nil
}

// Parallelism implements the interface Harness.
func (m *SQLITEHarness) Parallelism() int {
	return 1
}

// NewDatabase implements the interface Harness.
func (m *SQLITEHarness) NewDatabase(name string) sql.Database {
	return m.NewDatabases(name)[0]
}

// NewDatabases implements the interface Harness.
func (m *SQLITEHarness) NewDatabases(names ...string) []sql.Database {
	var dbs []sql.Database
	ctx := sql.NewEmptyContext()
	for _, name := range names {
		_ = m.shim.DropDatabase(ctx, name)
		err := m.shim.CreateDatabase(ctx, name)
		if err != nil {
			panic(err)
		}
		db, err := m.shim.Database(name)
		if err != nil {
			panic(err)
		}
		dbs = append(dbs, db)
	}
	return dbs
}

// NewDatabaseProvider implements the interface Harness.
func (m *SQLITEHarness) NewDatabaseProvider(dbs ...sql.Database) sql.MutableDatabaseProvider {
	return m.shim
}

// NewTable implements the interface Harness.
func (m *SQLITEHarness) NewTable(db sql.Database, name string, schema sql.PrimaryKeySchema) (sql.Table, error) {
	ctx := sql.NewEmptyContext()
	err := db.(sql.TableCreator).CreateTable(ctx, name, schema)
	if err != nil {
		debug.Dump("CreateTable err is ")
		debug.Dump(err.Error())
		return nil, err
	}
	tbl, ok, err := db.GetTableInsensitive(ctx, name)
	if err != nil {
		debug.Dump("GetTableInsensitive err is ")
		debug.Dump(err.Error())
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("successfully created table `%s` but could not retrieve", name)
	}
	return tbl, nil
}

// NewContext implements the interface Harness.
func (m *SQLITEHarness) NewContext() *sql.Context {
	return sql.NewEmptyContext()
}

// SkipQueryTest implements the interface SkippingHarness.
func (m *SQLITEHarness) SkipQueryTest(query string) bool {
	_, ok := m.skippedQueries[strings.ToLower(query)]
	return ok
}

// QueriesToSkip adds queries that should be skipped.
func (m *SQLITEHarness) QueriesToSkip(queries ...string) {
	for _, query := range queries {
		m.skippedQueries[strings.ToLower(query)] = struct{}{}
	}
}

// SupportsNativeIndexCreation implements the interface IndexHarness.
func (m *SQLITEHarness) SupportsNativeIndexCreation() bool {
	return true
}

// SupportsForeignKeys implements the interface ForeignKeyHarness.
func (m *SQLITEHarness) SupportsForeignKeys() bool {
	return true
}

// SupportsKeylessTables implements the interface KeylessTableHarness.
func (m *SQLITEHarness) SupportsKeylessTables() bool {
	return true
}

// Close closes the connection. This will drop all databases created and accessed during the tests.
func (m *SQLITEHarness) Close() {
	m.shim.Close()
}
