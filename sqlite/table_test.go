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
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Rock-liyi/p2pdb-store/sql"
	"github.com/Rock-liyi/p2pdb-store/sql/expression"
	"github.com/Rock-liyi/p2pdb-store/sql/parse"
	"github.com/Rock-liyi/p2pdb-store/sqlite"
	log "github.com/Rock-liyi/p2pdb/infrastructure/util/log"
)

func init() {

}

func TestTrimString(t *testing.T) {
	//require := require.New(t)
	var d = strings.TrimRight("SELECT * FROM p2pdb.test LIMIT 0, 200", "")
	log.Debug(d)

	s := "Hello world hello world"
	str := "hello"
	//var s = []string{"11","22","33"}

	//删除s尾部连续的包含在str中的字符串
	ret := strings.Replace(s, str, "", 1)
	log.Debug(ret) // Hello world hello world

}

func TestContainsString(t *testing.T) {
	log.Debug(strings.Contains("sqlite_autoindex_database_infomations_1", "sqlite1_autoindex_")) //true
	log.Debug(strings.Contains("wi", "widuu"))                                                   //false
}

func TestTablePartitionsCount(t *testing.T) {
	require := require.New(t)
	table := sqlite.NewPartitionedTable("foo", sql.PrimaryKeySchema{}, 5)
	count, err := table.PartitionCount(sql.NewEmptyContext())
	require.NoError(err)
	require.Equal(int64(5), count)
}

func TestTableName(t *testing.T) {
	require := require.New(t)
	s := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
	})

	table := sqlite.NewTable("test", s)
	require.Equal("test", table.Name())
}

func TestTableString(t *testing.T) {
	require := require.New(t)
	table := sqlite.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Int64, Nullable: false},
	}))
	require.Equal("foo", table.String())
}

type indexKeyValue struct {
	key   sql.Row
	value *sqlite.IndexValue
}

type dummyLookup struct {
	values map[string][]*sqlite.IndexValue
}

var _ sql.DriverIndexLookup = (*dummyLookup)(nil)

func (dummyLookup) Indexes() []string { return nil }

func (i dummyLookup) String() string {
	panic("index")
}

func (i *dummyLookup) Values(partition sql.Partition) (sql.IndexValueIter, error) {
	key := string(partition.Key())
	values, ok := i.values[key]
	if !ok {
		return nil, fmt.Errorf("wrong partition key %q", key)
	}

	return &dummyLookupIter{values: values}, nil
}

func (i *dummyLookup) Index() sql.Index {
	panic("not implemented")
}

func (i *dummyLookup) Ranges() sql.RangeCollection {
	panic("not implemented")
}

type dummyLookupIter struct {
	values []*sqlite.IndexValue
	pos    int
}

var _ sql.IndexValueIter = (*dummyLookupIter)(nil)

func (i *dummyLookupIter) Next(*sql.Context) ([]byte, error) {
	if i.pos >= len(i.values) {
		return nil, io.EOF
	}

	value := i.values[i.pos]
	i.pos++
	return sqlite.EncodeIndexValue(value)
}

func (i *dummyLookupIter) Close(_ *sql.Context) error { return nil }

var tests = []struct {
	name          string
	schema        sql.PrimaryKeySchema
	numPartitions int
	rows          []sql.Row

	filters          []sql.Expression
	expectedFiltered []sql.Row

	columns           []string
	expectedProjected []sql.Row

	expectedFiltersAndProjections []sql.Row

	indexColumns      []string
	expectedKeyValues []*indexKeyValue

	lookup          *dummyLookup
	partition       *sqlite.Partition
	expectedIndexed []sql.Row
}{
	{
		name: "test",
		schema: sql.NewPrimaryKeySchema(sql.Schema{
			&sql.Column{Name: "col1", Source: "test", Type: sql.Text, Nullable: false, Default: parse.MustStringToColumnDefaultValue(sql.NewEmptyContext(), `""`, sql.Text, false)},
			&sql.Column{Name: "col2", Source: "test", Type: sql.Int32, Nullable: false, Default: parse.MustStringToColumnDefaultValue(sql.NewEmptyContext(), "0", sql.Int32, false)},
			&sql.Column{Name: "col3", Source: "test", Type: sql.Int64, Nullable: false, Default: parse.MustStringToColumnDefaultValue(sql.NewEmptyContext(), "0", sql.Int64, false)},
		}),
		numPartitions: 2,
		rows: []sql.Row{
			sql.NewRow("a", int32(10), int64(100)),
			sql.NewRow("b", int32(10), int64(100)),
			sql.NewRow("c", int32(20), int64(100)),
			sql.NewRow("d", int32(20), int64(200)),
			sql.NewRow("e", int32(10), int64(200)),
			sql.NewRow("f", int32(20), int64(200)),
		},
		filters: []sql.Expression{
			expression.NewEquals(
				expression.NewGetFieldWithTable(1, sql.Int32, "test", "col2", false),
				expression.NewLiteral(int32(10), sql.Int32),
			),
		},
		expectedFiltered: []sql.Row{
			sql.NewRow("a", int32(10), int64(100)),
			sql.NewRow("b", int32(10), int64(100)),
			sql.NewRow("e", int32(10), int64(200)),
		},
		columns: []string{"col3", "col1"},
		expectedProjected: []sql.Row{
			sql.NewRow("a", nil, int64(100)),
			sql.NewRow("b", nil, int64(100)),
			sql.NewRow("c", nil, int64(100)),
			sql.NewRow("d", nil, int64(200)),
			sql.NewRow("e", nil, int64(200)),
			sql.NewRow("f", nil, int64(200)),
		},
		expectedFiltersAndProjections: []sql.Row{
			sql.NewRow("a", nil, int64(100)),
			sql.NewRow("b", nil, int64(100)),
			sql.NewRow("e", nil, int64(200)),
		},
		indexColumns: []string{"col1", "col3"},
		expectedKeyValues: []*indexKeyValue{
			{sql.NewRow("a", int64(100)), &sqlite.IndexValue{Key: "0", Pos: 0}},
			{sql.NewRow("c", int64(100)), &sqlite.IndexValue{Key: "0", Pos: 1}},
			{sql.NewRow("e", int64(200)), &sqlite.IndexValue{Key: "0", Pos: 2}},
			{sql.NewRow("b", int64(100)), &sqlite.IndexValue{Key: "1", Pos: 0}},
			{sql.NewRow("d", int64(200)), &sqlite.IndexValue{Key: "1", Pos: 1}},
			{sql.NewRow("f", int64(200)), &sqlite.IndexValue{Key: "1", Pos: 2}},
		},
		lookup: &dummyLookup{
			values: map[string][]*sqlite.IndexValue{
				"0": {
					{Key: "0", Pos: 0},
					{Key: "0", Pos: 1},
					{Key: "0", Pos: 2},
				},
				"1": {
					{Key: "1", Pos: 0},
					{Key: "1", Pos: 1},
					{Key: "1", Pos: 2},
				},
			},
		},
		partition: sqlite.NewPartition([]byte("0")),
		expectedIndexed: []sql.Row{
			{"a", nil, int64(100)},
			{"c", nil, int64(100)},
			{"e", nil, int64(200)},
		},
	},
}

func TestTable(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := sqlite.NewPartitionedTable(test.name, test.schema, test.numPartitions)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			ctx := sql.NewEmptyContext()
			pIter, err := table.Partitions(ctx)
			require.NoError(err)

			for i := 0; i < test.numPartitions; i++ {
				var p sql.Partition
				p, err = pIter.Next(ctx)
				require.NoError(err)

				var iter sql.RowIter
				ctx := sql.NewEmptyContext()
				iter, err = table.PartitionRows(ctx, p)
				require.NoError(err)

				var rows []sql.Row
				rows, err = sql.RowIterToRows(ctx, iter)
				require.NoError(err)

				expected := table.GetPartition(string(p.Key()))
				require.Len(rows, len(expected))

				for i, row := range rows {
					require.ElementsMatch(expected[i], row)
				}
			}

			_, err = pIter.Next(ctx)
			require.EqualError(err, io.EOF.Error())

		})
	}
}

func TestFiltered(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := sqlite.NewFilteredTable(test.name, test.schema)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			filtered := table.WithFilters(sql.NewEmptyContext(), test.filters)

			filteredRows := getAllRows(t, filtered)
			require.Len(filteredRows, len(test.expectedFiltered))
			for _, row := range filteredRows {
				require.Contains(test.expectedFiltered, row)
			}

		})
	}
}

func TestProjected(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := sqlite.NewPartitionedTable(test.name, test.schema, test.numPartitions)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			projected := table.WithProjection(test.columns)

			projectedRows := getAllRows(t, projected)
			require.Len(projectedRows, len(test.expectedProjected))
			for _, row := range projectedRows {
				require.Contains(test.expectedProjected, row)
			}
		})
	}
}

func TestFilterAndProject(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := sqlite.NewFilteredTable(test.name, test.schema)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			filtered := table.WithFilters(sql.NewEmptyContext(), test.filters)
			projected := filtered.(*sqlite.FilteredTable).WithProjection(test.columns)

			rows := getAllRows(t, projected)
			require.Len(rows, len(test.expectedFiltersAndProjections))
			for _, row := range rows {
				require.Contains(test.expectedFiltersAndProjections, row)
			}
		})
	}
}

func TestIndexed(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := sqlite.NewPartitionedTable(test.name, test.schema, test.numPartitions)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			projected := table.WithProjection(test.columns)
			indexed := projected.(*sqlite.Table).WithIndexLookup(test.lookup)

			ctx := sql.NewEmptyContext()
			iter, err := indexed.PartitionRows(ctx, test.partition)
			require.NoError(err)

			rows, err := sql.RowIterToRows(ctx, iter)
			require.NoError(err)

			require.Equal(rows, test.expectedIndexed)
		})
	}
}

func getAllRows(t *testing.T, table sql.Table) []sql.Row {
	var require = require.New(t)

	ctx := sql.NewEmptyContext()
	pIter, err := table.Partitions(ctx)
	require.NoError(err)
	allRows := []sql.Row{}
	for {
		p, err := pIter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}

			require.NoError(err)
		}

		iter, err := table.PartitionRows(ctx, p)
		require.NoError(err)

		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(err)

		allRows = append(allRows, rows...)
	}

	return allRows
}

func TestTableIndexKeyValueIter(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var require = require.New(t)

			table := sqlite.NewPartitionedTable(test.name, test.schema, test.numPartitions)
			for _, row := range test.rows {
				require.NoError(table.Insert(sql.NewEmptyContext(), row))
			}

			pIter, err := table.IndexKeyValues(
				sql.NewEmptyContext(),
				[]string{test.schema.Schema[0].Name, test.schema.Schema[2].Name},
			)
			require.NoError(err)

			ctx := sql.NewEmptyContext()

			var iter sql.IndexKeyValueIter
			idxKVs := []*indexKeyValue{}
			for {
				if iter == nil {
					_, iter, err = pIter.Next(ctx)
					if err != nil {
						if err == io.EOF {
							iter = nil
							break
						}

						require.NoError(err)
					}
				}

				row, data, err := iter.Next(ctx)
				if err != nil {
					if err == io.EOF {
						iter = nil
						continue
					}

					require.NoError(err)
				}

				value, err := sqlite.DecodeIndexValue(data)
				require.NoError(err)

				idxKVs = append(idxKVs, &indexKeyValue{key: row, value: value})
			}

			require.Len(idxKVs, len(test.expectedKeyValues))
			for i, e := range test.expectedKeyValues {
				require.Equal(e, idxKVs[i])
			}
		})
	}
}
