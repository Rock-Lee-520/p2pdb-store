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

package sqlite

import (
	dbsql "database/sql"
	"os"
	"path/filepath"
	"strings"

	//	log "github.com/sirupsen/logrus"

	"github.com/Rock-liyi/p2pdb-store/entity"
	"github.com/Rock-liyi/p2pdb-store/entity/value_object"
	"github.com/Rock-liyi/p2pdb-store/event"
	"github.com/Rock-liyi/p2pdb-store/sql"
	conf "github.com/Rock-liyi/p2pdb/infrastructure/util/config"
	"github.com/dolthub/vitess/go/sqltypes"
	debug "github.com/favframework/debug"

	//dbParse "github.com/Rock-liyi/p2pdb-store/sql/parse"

	_ "github.com/mattn/go-sqlite3"
	//"github.com/opentracing/opentracing-go/log"
	log "github.com/Rock-liyi/p2pdb/infrastructure/util/log"
)

// Database is an in-sqlite database.
type Database struct {
	*BaseDatabase
	connection *dbsql.DB
	address    string
	views      map[string]string
}

type SqliteDatabase interface {
	sql.Database
	AddTable(name string, t sql.Table)
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)
var _ sql.TriggerDatabase = (*Database)(nil)
var _ sql.StoredProcedureDatabase = (*Database)(nil)
var _ sql.ViewDatabase = (*Database)(nil)

// BaseDatabase is an in-sqlite database that can't store views, only for testing the engine
type BaseDatabase struct {
	connection        *dbsql.DB
	address           string
	name              string
	tables            map[string]sql.Table
	triggers          []sql.TriggerDefinition
	storedProcedures  []sql.StoredProcedureDetails
	primaryKeyIndexes bool
}

var _ SqliteDatabase = (*Database)(nil)
var _ SqliteDatabase = (*BaseDatabase)(nil)

// NewDatabase creates a new database with the given name.
func NewDatabase(name string) *Database {
	database := NewViewlessDatabase(name)
	return &Database{
		BaseDatabase: database,
		connection:   database.connection,
		address:      database.address,
		views:        make(map[string]string),
	}
}

// NewViewlessDatabase creates a new database that doesn't persist views. Used only for testing. Use NewDatabase.
func NewViewlessDatabase(name string) *BaseDatabase {

	dataPath := conf.GetDataPath()
	// do something here to set environment depending on an environment variable
	// or command-line flag
	if dataPath != "" {
		dataPath = dataPath + "/"
	}
	debug.Dump(dataPath)
	binary, _ := os.Getwd()
	root := filepath.Dir(binary)
	if root != "" && dataPath == "" {
		dataPath = root + "/"
	}
	debug.Dump(dataPath + name + ".db")
	address := dataPath + name + ".db"
	db, err := dbsql.Open("sqlite3", address)
	if err != nil {
		log.Error(err)
	}

	// rows, err := db.Query("select name from sqlite_master  where name !='sqlite_sequence' ")
	// debug.Dump("========->1 select name from sqlite_master where name !='sqlite_sequence'")
	// if err != nil {
	// 	log.Error(err)
	// }
	// debug.Dump(rows)
	// //panic(err)
	//defualt create database
	dbSql := "create database " + name

	_, err = db.Exec(dbSql)

	if err != nil {
		//debug.Dump(err)
		log.Error(err)
	}

	//if you   close the connection , the db will not work anymore
	//defer db.Close()

	database := BaseDatabase{
		name:       name,
		tables:     map[string]sql.Table{},
		connection: db,
		address:    address,
	}
	//database.connection = db
	return &database
}

// func checkErr(err error) {
// 	if err != nil {
// 		panic(err)
// 	}
// }

// EnablePrimaryKeyIndexes causes every table created in this database to use an index on its primary partitionKeys
func (d *BaseDatabase) EnablePrimaryKeyIndexes() {
	d.primaryKeyIndexes = true
}

// Name returns the database name.
func (d *BaseDatabase) Name() string {
	return d.name
}

// Connection returns the database connection.
func (d *BaseDatabase) Connection() *dbsql.DB {
	return d.connection
}

func (d *BaseDatabase) Address() string {
	return d.address
}

// Tables returns all tables in the database.
func (d *BaseDatabase) Tables() map[string]sql.Table {
	debug.Dump("========-> Tables method")

	//Get all of table , it not include sqlite_sequence table name
	// rows, err := d.connection.Query("select name from sqlite_master where name !='sqlite_sequence'")
	rows, err := d.connection.Query("select name from sqlite_master  where name !='sqlite_sequence' ")
	debug.Dump("========-> select name from sqlite_master where name !='sqlite_sequence'")
	if err != nil {
		log.Error(err)
	}

	var tableName string
	//var Table sql.Table
	for rows.Next() {

		err := rows.Scan(&tableName)
		if err != nil {
			log.Error(err)
		}
		if strings.Contains(tableName, "sqlite_autoindex_") == false {
			//debug.Dump(tableName)
			d.tables[tableName] = d.getTable(tableName)
			//d.tables[tableName] = Table
		}

	}
	//debug.Dump(d.tables)
	//it  must to be closed or else  will stuck
	rows.Close()
	return d.tables
}

// Tables returns all tables in the database.
func (d *BaseDatabase) InitTables() {
	debug.Dump("========-> InitTables method")

	//Get all of table , it not include sqlite_sequence table name
	// rows, err := d.connection.Query("select name from sqlite_master where name !='sqlite_sequence'")
	rows, err := d.connection.Query("select name from sqlite_master  where name !='sqlite_sequence' ")
	debug.Dump("========-> select name from sqlite_master where name !='sqlite_sequence'")
	if err != nil {
		log.Error(err)
	}

	var tableName string
	//var Table sql.Table
	for rows.Next() {

		err := rows.Scan(&tableName)
		if err != nil {
			log.Error(err)
		}
		if strings.Contains(tableName, "sqlite_autoindex_") == false {
			//debug.Dump(tableName)
			var newTable = d.getTable(tableName)
			d.AddTable(tableName, newTable)
			ctx := sql.NewEmptyContext()
			newTable.ApplyEdits(ctx)
		}

	}

	//it  must to be closed or else  will stuck
	rows.Close()
}

func (d *BaseDatabase) ParseColumnStringToSqlType(atype string) (sql.Type, error) {
	// debug.Dump("========ParseColumnStringToSqlType")
	types := []struct {
		columnType      string
		expectedSqlType sql.Type
	}{
		{
			"tinyint",
			sql.Int8,
		},
		{
			"SMALLINT",
			sql.Int16,
		},
		{
			"MeDiUmInT",
			sql.Int24,
		},
		{
			"INT",
			sql.Int32,
		},
		{
			"INT16",
			sql.Int16,
		},
		{
			"INT32",
			sql.Int32,
		},
		{
			"INT64",
			sql.Int64,
		},
		{
			"BIGINT",
			sql.Int64,
		},
		{
			"INTEGER",
			sql.Int64,
		},
		{
			"TINYINT UNSIGNED",
			sql.Uint8,
		},
		{
			"SMALLINT UNSIGNED",
			sql.Uint16,
		},
		{
			"MEDIUMINT UNSIGNED",
			sql.Uint24,
		},
		{
			"INT UNSIGNED",
			sql.Uint32,
		},
		{
			"BIGINT UNSIGNED",
			sql.Uint64,
		},
		{
			"BOOLEAN",
			sql.Int8,
		},
		{
			"FLOAT",
			sql.Float32,
		},
		{
			"DOUBLE",
			sql.Float64,
		},
		{
			"REAL",
			sql.Float64,
		},
		{
			"DECIMAL",
			sql.MustCreateDecimalType(10, 0),
		},
		{
			"DECIMAL(22)",
			sql.MustCreateDecimalType(22, 0),
		},
		{
			"DECIMAL(55, 13)",
			sql.MustCreateDecimalType(55, 13),
		},
		{
			"DEC(34, 2)",
			sql.MustCreateDecimalType(34, 2),
		},
		{
			"FIXED(4, 4)",
			sql.MustCreateDecimalType(4, 4),
		},
		{
			"BIT(31)",
			sql.MustCreateBitType(31),
		},
		{
			"TINYBLOB",
			sql.TinyBlob,
		},
		{
			"BLOB",
			sql.Blob,
		},
		{
			"MEDIUMBLOB",
			sql.MediumBlob,
		},
		{
			"LONGBLOB",
			sql.LongBlob,
		},
		{
			"TINYTEXT",
			sql.TinyText,
		},
		{
			"TEXT",
			sql.Text,
		},
		{
			"MEDIUMTEXT",
			sql.MediumText,
		},
		{
			"LONGTEXT",
			sql.LongText,
		},
		{
			"CHAR(5)",
			sql.MustCreateStringWithDefaults(sqltypes.Char, 5),
		},
		{
			"VARCHAR",
			sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
		},

		{
			"VARCHAR(255)",
			sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255),
		},
		{
			"VARCHAR(300) COLLATE cp1257_lithuanian_ci",
			sql.MustCreateString(sqltypes.VarChar, 300, sql.Collation_cp1257_lithuanian_ci),
		},
		{
			"BINARY(6)",
			sql.MustCreateBinary(sqltypes.Binary, 6),
		},
		{
			"VARBINARY(256)",
			sql.MustCreateBinary(sqltypes.VarBinary, 256),
		},
		{
			"YEAR",
			sql.Year,
		},
		{
			"DATE",
			sql.Date,
		},
		{
			"TIME",
			sql.Time,
		},
		{
			"TIMESTAMP",
			sql.Timestamp,
		},
		{
			"DATETIME",
			sql.Datetime,
		},
		{
			"JSON",
			sql.Text,
		},
	}

	for _, t := range types {
		// t.Run(test.columnType, func(t *testing.T) {
		if atype == t.columnType {
			return t.expectedSqlType, nil
		}
	}
	debug.Dump("can not find any sql type")
	return nil, nil
}

type ParseColumn struct {
	Cid        int8
	Name       string
	Type       sql.Type
	Notnull    string
	Dflt_value string
	Pk         string
}

// getTable select a  Table with the given name in the database .
func (d *BaseDatabase) getTable(name string) *Table {
	log.Info("======= getTable method start")
	// rowss, err := d.connection.QueryContext(sql.NewEmptyContext(), "PRAGMA table_info('"+name+"')")
	// debug.Dump(rowss.Columns())
	// if err != nil {
	// 	log.Error(err)
	// }
	rows, err := d.connection.Query("PRAGMA table_info('" + name + "')")

	// rows, err := d.connection.Query("select name from sqlite_master  where name !='sqlite_sequence' ")
	// rows, err := d.connection.Query("select * from test_table5 ")

	// debug.Dump("========-> PRAGMA table_info('" + name + "')")
	if err != nil {
		log.Error(err)
	}

	data := make(map[string]string)
	for rows.Next() {
		var Cid string
		var Name string
		var Type string
		var Notnull string
		var Dflt_value string
		var Pk string

		err := rows.Scan(&Cid, &Name, &Type, &Notnull, &Dflt_value, &Pk)

		if err != nil {
			log.Error(err)
		}
		// debug.Dump("===========aType")
		// debug.Dump(Name)
		// debug.Dump(Type)

		if Type == "" || Name == "" {
			break
		}
		data[Name] = Type

	}

	newSchemaWithoutCol := make(sql.Schema, len(data))

	i := 0
	for Name, Type := range data {
		var sqlType sql.Type
		sqlType, err = d.ParseColumnStringToSqlType(Type)
		if err != nil {
			log.Error(err)
		}
		newSchemaWithoutCol[i] = &sql.Column{
			Name:   Name,
			Type:   sqlType,
			Source: name,
		}
		i++
	}

	// debug.Dump("========newSchemaWithoutCol is ")
	// debug.Dump(newSchemaWithoutCol)
	schema := sql.NewPrimaryKeySchema(newSchemaWithoutCol)
	log.Info("======= getTable method end")
	return NewPartitionedTable(name, schema, 0)
}

func (d *BaseDatabase) GetTableSchema(name string) (sql.PrimaryKeySchema, error) {
	debug.Dump("======= getTable method")
	// rowss, err := d.connection.QueryContext(sql.NewEmptyContext(), "PRAGMA table_info('"+name+"')")
	// debug.Dump(rowss.Columns())
	// if err != nil {
	// 	log.Error(err)
	// }
	rows, err := d.connection.Query("PRAGMA table_info('" + name + "')")

	// rows, err := d.connection.Query("select name from sqlite_master  where name !='sqlite_sequence' ")
	// rows, err := d.connection.Query("select * from test_table5 ")

	// debug.Dump("========-> PRAGMA table_info('" + name + "')")
	if err != nil {
		log.Error(err)
	}

	data := make(map[string]string)
	for rows.Next() {
		var Cid string
		var Name string
		var Type string
		var Notnull string
		var Dflt_value string
		var Pk string

		err := rows.Scan(&Cid, &Name, &Type, &Notnull, &Dflt_value, &Pk)

		if err != nil {
			log.Error(err)
		}
		debug.Dump("===========aType")
		debug.Dump(Name)
		debug.Dump(Type)

		if Type == "" || Name == "" {
			break
		}
		data[Name] = Type

	}
	if len(data) == 0 {
		return sql.PrimaryKeySchema{}, sql.NewEmptyContext().Err()
	}
	newSchemaWithoutCol := make(sql.Schema, len(data))

	i := 0
	for Name, Type := range data {
		var sqlType sql.Type
		sqlType, err = d.ParseColumnStringToSqlType(Type)
		if err != nil {
			log.Error(err)
		}
		newSchemaWithoutCol[i] = &sql.Column{
			Name:   Name,
			Type:   sqlType,
			Source: name,
		}
		i++
	}

	debug.Dump("========newSchemaWithoutCol is ")
	debug.Dump(newSchemaWithoutCol)
	schema := sql.NewPrimaryKeySchema(newSchemaWithoutCol)

	return schema, nil
}

func (d *BaseDatabase) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	debug.Dump("========-> GetTableInsensitive")
	//ctx := newPersistedSqlContext()
	// debug.Dump(ctx.Query())
	// debug.Dump(ctx.RawStatement())
	// //debug.Dump("==========GetCurrentDatabase")
	ctx.Session.SetCurrentDatabase(d.Name())
	ctx.Session.SetAddress(d.Address())
	ctx.Session.SetConnection(d.Connection())
	debug.Dump(ctx.Session.GetCurrentDatabase())
	debug.Dump(ctx.Session.Address())
	//debug.Dump("========-> GetTableInsensitive sql.Row")
	debug.Dump("========-> GetTableInsensitive tables1")
	debug.Dump(d.tables)
	// //	d.tables = d.Tables()

	tbl, ok := sql.GetTableInsensitive(tblName, d.tables)
	if ok == false {
		debug.Dump("GetTableInsensitive is false")
		// 	schema, err := d.GetTableSchema(tblName)
		// 	if err == nil {
		// 		table := NewTable(tblName, sql.NewPrimaryKeySchema(schema.Schema))
		// 		// debug.Dump(table)
		// 		d.AddTable(tblName, table)

		// 		debug.Dump("========-> GetTableInsensitive tables2")
		// 		debug.Dump(d.tables)
		// 	}

	}

	// tbl, ok = sql.GetTableInsensitive(tblName, d.tables)
	debug.Dump("========-> GetTableInsensitive end")
	return tbl, ok, nil
}

func (d *BaseDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	tblNames := make([]string, 0, len(d.tables))
	for k := range d.tables {
		tblNames = append(tblNames, k)
	}

	return tblNames, nil
}

func (d *BaseDatabase) GetTableNamesBak(ctx *sql.Context) ([]string, error) {
	debug.Dump("========-> GetTableNames start")

	d.tables = d.Tables()
	tblNames := make([]string, 0, len(d.tables))
	for k := range d.tables {
		tblNames = append(tblNames, k)
	}

	debug.Dump("========-> GetTableNames end")

	return tblNames, nil
}

// HistoryDatabase is a test-only VersionedDatabase implementation. It only supports exact lookups, not AS OF queries
// between two revisions. It's constructed just like its non-versioned sibling, but it can receive updates to particular
// tables via the AddTableAsOf method. Consecutive calls to AddTableAsOf with the same table must install new versions
// of the named table each time, with ascending version identifiers, for this to work.
type HistoryDatabase struct {
	*Database
	Revisions    map[string]map[interface{}]sql.Table
	currRevision interface{}
}

var _ sql.VersionedDatabase = (*HistoryDatabase)(nil)

func (db *HistoryDatabase) GetTableInsensitiveAsOf(ctx *sql.Context, tblName string, time interface{}) (sql.Table, bool, error) {
	debug.Dump("============GetTableInsensitiveAsOf start")
	table, ok := db.Revisions[strings.ToLower(tblName)][time]
	if ok {
		return table, true, nil
	}

	// If we have revisions for the named table, but not the named revision, consider it not found.
	if _, ok := db.Revisions[strings.ToLower(tblName)]; ok {
		return nil, false, sql.ErrTableNotFound.New(tblName)
	}
	debug.Dump("============GetTableInsensitiveAsOf end")
	// Otherwise (this table has no revisions), return it as an unversioned lookup
	return db.GetTableInsensitive(ctx, tblName)
}

func (db *HistoryDatabase) GetTableNamesAsOf(ctx *sql.Context, time interface{}) ([]string, error) {
	debug.Dump("============GetTableNamesAsOf")
	// TODO: this can't make any queries fail (only used for error messages on table lookup failure), but would be nice
	//  to support better.
	return db.GetTableNames(ctx)
}

func NewHistoryDatabase(name string) *HistoryDatabase {
	return &HistoryDatabase{
		Database:  NewDatabase(name),
		Revisions: make(map[string]map[interface{}]sql.Table),
	}
}

// Adds a table with an asOf revision key. The table given becomes the current version for the name given.
func (db *HistoryDatabase) AddTableAsOf(name string, t sql.Table, asOf interface{}) {
	// TODO: this won't handle table names that vary only in case
	if _, ok := db.Revisions[strings.ToLower(name)]; !ok {
		db.Revisions[strings.ToLower(name)] = make(map[interface{}]sql.Table)
	}

	db.Revisions[strings.ToLower(name)][asOf] = t
	db.tables[name] = t
}

// AddTable adds a new table to the database.
func (d *BaseDatabase) AddTable(name string, t sql.Table) {
	debug.Dump("=======AddTable method")

	table := NewTable(name, sql.NewPrimaryKeySchema(t.Schema()))

	table.createTableToSqlite(table)
	d.tables[name] = t
}

// CreateTable creates a table with the given name and schema
func (d *BaseDatabase) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema) error {
	debug.Dump("=======CreateTable-1 start")
	_, ok := d.tables[name]
	if ok {
		return sql.ErrTableAlreadyExists.New(name)
	}

	table := NewTable(name, schema)

	if d.primaryKeyIndexes {
		table.EnablePrimaryKeyIndexes()
	}

	if table.sqlStatement != "" {
		table.createTableToSqlite(table)
	}

	d.tables[name] = table
	//debug.Dump(d.tables)
	debug.Dump("=======CreateTable-1 end")
	return nil
}

// func (d *BaseDatabase) createTableToSqlite(table *Table) {
// 	//it create a  file table by sqlite
// 	debug.Dump("=========createTableToSqlite method")
// 	debug.Dump(table.sqlStatement)
// 	_, err := d.connection.Exec(table.sqlStatement)

// 	if err != nil {
// 		log.Error(err)
// 	}
// 	event.PublishSyncEvent(commonEvent.StoreCreateTableEvent, table.sqlStatement)
// 	debug.Dump("=========createTableToSqlite method end")
// }

// DropTable drops the table with the given name
func (d *BaseDatabase) DropTable(ctx *sql.Context, name string) error {
	log.Debug("=========DropTable method start")
	d.tables = d.Tables()
	_, ok := d.tables[name]
	if !ok {
		return sql.ErrTableNotFound.New(name)
	}
	//delete table from  in the database
	_, err := d.connection.Exec("DROP TABLE  " + name)
	if err != nil {
		//debug.Dump("show error=========")
		log.Error(err)
	}
	var eventData = entity.Data{TableName: name, SQLStatement: "DROP TABLE  " + name, DDLActionType: value_object.TABLE, DDLType: value_object.DROP}
	event.PublishSyncEvent(value_object.StoreDropTableEvent, eventData)

	delete(d.tables, name)
	log.Debug("=========DropTable method end")
	return nil
}

func (d *BaseDatabase) RenameTable(ctx *sql.Context, oldName, newName string) error {
	log.Debug("=========RenameTable method start")

	//defulat code
	tbl, ok := d.tables[oldName]
	if !ok {
		// Should be impossible (engine already checks this condition)
		return sql.ErrTableNotFound.New(oldName)
	}

	_, ok = d.tables[newName]
	if ok {
		return sql.ErrTableAlreadyExists.New(newName)
	}

	//sqlite code
	SQLStatement := "ALTER TABLE " + oldName + "  RENAME TO " + newName
	_, err := d.connection.Exec(SQLStatement)
	if err != nil {
		//debug.Dump("show error=========")
		log.Error(err)
	}

	var eventData = entity.Data{TableName: oldName, SQLStatement: SQLStatement, DDLActionType: value_object.TABLE, DDLType: value_object.ALTER_TABLE_RENAME}
	event.PublishSyncEvent(value_object.StoreAlterTableRenameEvent, eventData)

	tbl.(*Table).name = newName
	d.tables[newName] = tbl
	delete(d.tables, oldName)
	log.Debug("=========RenameTable method end")
	return nil
}

func (d *BaseDatabase) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) {
	var triggers []sql.TriggerDefinition
	for _, def := range d.triggers {
		triggers = append(triggers, def)
	}
	return triggers, nil
}

func (d *BaseDatabase) CreateTrigger(ctx *sql.Context, definition sql.TriggerDefinition) error {
	d.triggers = append(d.triggers, definition)
	return nil
}

func (d *BaseDatabase) DropTrigger(ctx *sql.Context, name string) error {
	found := false
	for i, trigger := range d.triggers {
		if trigger.Name == name {
			d.triggers = append(d.triggers[:i], d.triggers[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return sql.ErrTriggerDoesNotExist.New(name)
	}
	return nil
}

// GetStoredProcedures implements sql.StoredProcedureDatabase
func (d *BaseDatabase) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	var spds []sql.StoredProcedureDetails
	for _, spd := range d.storedProcedures {
		spds = append(spds, spd)
	}
	return spds, nil
}

// SaveStoredProcedure implements sql.StoredProcedureDatabase
func (d *BaseDatabase) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	loweredName := strings.ToLower(spd.Name)
	for _, existingSpd := range d.storedProcedures {
		if strings.ToLower(existingSpd.Name) == loweredName {
			return sql.ErrStoredProcedureAlreadyExists.New(spd.Name)
		}
	}
	d.storedProcedures = append(d.storedProcedures, spd)
	return nil
}

// DropStoredProcedure implements sql.StoredProcedureDatabase
func (d *BaseDatabase) DropStoredProcedure(ctx *sql.Context, name string) error {
	loweredName := strings.ToLower(name)
	found := false
	for i, spd := range d.storedProcedures {
		if strings.ToLower(spd.Name) == loweredName {
			d.storedProcedures = append(d.storedProcedures[:i], d.storedProcedures[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return sql.ErrStoredProcedureDoesNotExist.New(name)
	}
	return nil
}

func (d *Database) CreateView(ctx *sql.Context, name string, selectStatement string) error {
	_, ok := d.views[name]
	if ok {
		return sql.ErrExistingView.New(name)
	}

	d.views[name] = selectStatement
	return nil
}

func (d *Database) DropView(ctx *sql.Context, name string) error {
	_, ok := d.views[name]
	if !ok {
		return sql.ErrViewDoesNotExist.New(name)
	}

	delete(d.views, name)
	return nil
}

func (d *Database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	debug.Dump("====AllViews")
	var views []sql.ViewDefinition
	for name, def := range d.views {
		views = append(views, sql.ViewDefinition{
			Name:           name,
			TextDefinition: def,
		})
	}
	return views, nil
}

func (d *Database) GetView(ctx *sql.Context, viewName string) (string, bool, error) {
	viewDef, ok := d.views[viewName]
	return viewDef, ok, nil
}

type ReadOnlyDatabase struct {
	*HistoryDatabase
}

var _ sql.ReadOnlyDatabase = ReadOnlyDatabase{}

func NewReadOnlyDatabase(name string) ReadOnlyDatabase {
	h := NewHistoryDatabase(name)
	return ReadOnlyDatabase{h}
}

func (d ReadOnlyDatabase) IsReadOnly() bool {
	return true
}
