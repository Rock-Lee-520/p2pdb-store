// Copyright 2021 P2PDB, Inc.
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

import "github.com/kkguan/p2pdb-store/sql"

type GlobalsMap = map[string]interface{}
type InSqlitePersistedSession struct {
	sql.Session
	persistedGlobals GlobalsMap
}

// NewInSqlitePersistedSession is a sql.PersistableSession that writes global variables to an im-memory map
func NewInSqlitePersistedSession(sess sql.Session, persistedGlobals GlobalsMap) *InSqlitePersistedSession {
	return &InSqlitePersistedSession{Session: sess, persistedGlobals: persistedGlobals}
}

// PersistGlobal implements sql.PersistableSession
func (s *InSqlitePersistedSession) PersistGlobal(sysVarName string, value interface{}) error {
	sysVar, _, ok := sql.SystemVariables.GetGlobal(sysVarName)
	if !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	val, err := sysVar.Type.Convert(value)
	if err != nil {
		return err
	}
	s.persistedGlobals[sysVarName] = val
	return nil
}

// RemovePersistedGlobal implements sql.PersistableSession
func (s *InSqlitePersistedSession) RemovePersistedGlobal(sysVarName string) error {
	if _, _, ok := sql.SystemVariables.GetGlobal(sysVarName); !ok {
		return sql.ErrUnknownSystemVariable.New(sysVarName)
	}
	delete(s.persistedGlobals, sysVarName)
	return nil
}

// RemoveAllPersistedGlobals implements sql.PersistableSession
func (s *InSqlitePersistedSession) RemoveAllPersistedGlobals() error {
	s.persistedGlobals = GlobalsMap{}
	return nil
}

// RemoveAllPersistedGlobals implements sql.PersistableSession
func (s *InSqlitePersistedSession) GetPersistedValue(k string) (interface{}, error) {
	return s.persistedGlobals[k], nil
}
