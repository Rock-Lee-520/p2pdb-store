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

package sql

import (
	"io"

	debug "github.com/favframework/debug"
)

// TableRowIter is an iterator over the partitions in a table.
type TableRowIter struct {
	table      Table
	partitions PartitionIter
	partition  Partition
	rows       RowIter
}

// NewTableRowIter returns a new iterator over the rows in the partitions of the table given.
func NewTableRowIter(ctx *Context, table Table, partitions PartitionIter) *TableRowIter {
	return &TableRowIter{table: table, partitions: partitions}
}

func (i *TableRowIter) Next(ctx *Context) (Row, error) {
	debug.Dump("============TableRowIter Next start")
	debug.Dump(ctx.RawStatement())
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if i.partition == nil {
		partition, err := i.partitions.Next(ctx)
		if err != nil {
			if err == io.EOF {
				if e := i.partitions.Close(ctx); e != nil {
					return nil, e
				}
			}

			return nil, err
		}

		i.partition = partition
	}
	debug.Dump("============TableRowIter partition1   start")
	debug.Dump(i.partition.Key())
	debug.Dump("============TableRowIter partition1 end")
	if i.rows == nil {
		rows, err := i.table.PartitionRows(ctx, i.partition)
		if err != nil {
			return nil, err
		}

		i.rows = rows
	}
	debug.Dump("============TableRowIter Next  row1 start")
	debug.Dump(i.rows)
	debug.Dump("============TableRowIter Next row1 end")
	row, err := i.rows.Next(ctx)
	debug.Dump("============TableRowIter Next  row2 start")
	debug.Dump(row)
	debug.Dump("============TableRowIter Next row2 end")

	if err != nil && err == io.EOF {
		debug.Dump(err)
		if err = i.rows.Close(ctx); err != nil {
			return nil, err
		}

		i.partition = nil
		i.rows = nil

		return i.Next(ctx)
	}
	debug.Dump("============TableRowIter Next end")
	return row, err
}

func (i *TableRowIter) Close(ctx *Context) error {
	if i.rows != nil {
		if err := i.rows.Close(ctx); err != nil {
			_ = i.partitions.Close(ctx)
			return err
		}
	}
	return i.partitions.Close(ctx)
}
