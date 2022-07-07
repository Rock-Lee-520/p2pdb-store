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

package function

import (
	"fmt"

	"github.com/Rock-liyi/p2pdb-store/sql"
	"github.com/Rock-liyi/p2pdb-store/sql/expression"
)

// ArrayLength returns the length of an array.
type ArrayLength struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*ArrayLength)(nil)

// NewArrayLength creates a new ArrayLength UDF.
func NewArrayLength(array sql.Expression) sql.Expression {
	return &ArrayLength{expression.UnaryExpression{Child: array}}
}

// FunctionName implements sql.FunctionExpression
func (f *ArrayLength) FunctionName() string {
	return "array_length"
}

// Description implements sql.FunctionExpression
func (f *ArrayLength) Description() string {
	return "if the json representation is an array, this function returns its size."
}

// Type implements the Expression interface.
func (*ArrayLength) Type() sql.Type {
	return sql.Int32
}

func (f *ArrayLength) String() string {
	return fmt.Sprintf("array_length(%s)", f.Child)
}

// WithChildren implements the Expression interface.
func (f *ArrayLength) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}
	return NewArrayLength(children[0]), nil
}

// Eval implements the Expression interface.
func (f *ArrayLength) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	t := f.Child.Type()
	if !sql.IsArray(t) && !sql.IsJSON(t) {
		return nil, nil
	}

	child, err := f.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if child == nil {
		return nil, nil
	}

	if val, ok := child.(sql.JSONValue); ok {
		js, err := val.Unmarshall(ctx)
		if err != nil {
			return nil, err
		}
		child = js.Val
	}

	array, ok := child.([]interface{})
	if !ok {
		return nil, nil
	}

	return int32(len(array)), nil
}
