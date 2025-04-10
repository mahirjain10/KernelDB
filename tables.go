package main

import (
	"encoding/json"
	"fmt"
)

// Instead of storing multiple tables in multiple btree
// We are going to insert all the tables in a single btree
// Now to segregate which data belongs to which tables
// we add a prefix to the key (which includes column data like an ID),
// and this key maps to the values (the row data).

const (
	TYPE_BYTES = 1 // any length / aribitrary length of string
	TYPE_INT64 = 2 // integer
)

const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 //only add new keys
)

type UpdateReq struct {
	tree *BTree

	// out
	Added   bool // added a new key
	Updated bool // added a new key or updated an old key
	// in

	Old  []byte // the value before the update
	Key  []byte //key to insert
	Val  []byte // val associated to key
	Mode int    // mode insert / update or upsert
}
type DB struct {
	Path string
	kv   KV
}

// table cell

type Value struct {
	Type uint32 // tagged uinon
	I64  int64  // stores int64
	Str  []byte // stores string
}

// IMP : A tagged union is a way to store multiple possible data types in one place, with a "tag" to indicate which type is active.

// table row
type Record struct {
	Cols []string // list column names
	Vals []Value  // list of values
}

// add a string to a row

// column name =["name","email"]
// value = [{"mahir,"mahir@gmail.com},{"neel,neel@gmail.com"}]
func (rec *Record) AddStr(col string, val []byte) *Record {
	// add column name to the row
	rec.Cols = append(rec.Cols, col)
	// add string value to the row
	rec.Vals = append(rec.Vals, Value{Type: 1, Str: val})
	return rec
}

func (rec *Record) AddInt64(col string, val int64) *Record
func (rec *Record) Get(col string) *Value

// Schema design

type TableDef struct {
	// user defined
	Name  string
	Types []uint32 // column types (in our case int or string)
	Cols  []string // column names
	Pkeys int      // tells which column is / are primary keys from start
	// primary key can be 0 that mean no primary key
	// ["id","email","name"] = id and email are primary key ,if Pkeys = 2
	// auto-assigned B-tree key prefixes for different tables
	Prefixes []uint32
	Indexes  [][]string // the first index is the primary key

	//internal
	db     *DB
	tdef   *TableDef // which index ?
	index  *BIter    // Btree iterator
	keyEnd []byte    // the encoded key2

}

// predefined internal tabe
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	Pkeys:  1,
}

var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	Pkeys:  1,
}

// get and return a single row by primary key
func (db *DB) Get(table string, rec *Record) (bool, error) {
	// get the table desc first to understnd the data to retrieve
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found :%s", table)
	}

	// provide the tabl desc/definition to get the data
	return dbGet(db, tdef, rec)
}

//

func getTableDef(db *DB, name string) *TableDef {
	//create empty record //add the name field with the binary data
	rec := (&Record{}).AddStr("name", []byte(name))

	ok, err := dbGet(db, TDEF_TABLE, rec) //query internal system table
	assert(err == nil)
	if !ok {
		return nil // table not found
	}

	tdef := &TableDef{}
	// unmarshall the table def
	err = json.Unmarshal(rec.Get("def").Str, tdef) //decode JSON scehma
	assert(err == nil)
	return tdef //return table schema
}

func (db *DB) Insert(table string, rec Record) (bool, error)
func (db *DB) Update(table string, rec Record) (bool, error)
func (db *DB) Upsert(table string, rec Record) (bool, error)
func (tx *DBTX) Set(table string, rec Record, mode int) (bool, error)
// func (db *DB) Delete(table string, rec Record) (bool, error)
func (tx *DBTX) Delete(table string, rec Record) (bool, error)

func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	// check if the record has primary key
	values, err := checkRecord(tdef, *rec, tdef.Pkeys)
	if err != nil {
		return false, err
	}
	// takes primary key and add a prefix using to primary key
	// which will be used to lookup for data
	key := encodeKey(nil, tdef.Prefix, values[:tdef.Pkeys])

	// get if the key exists if yes  then give back the encoded data
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}
	// 4. decode the value into columns
	for i := tdef.Pkeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}
	decodeValues(val, values[tdef.Pkeys:])
	rec.Cols = tdef.Cols
	rec.Vals = values
	return true, nil
}

func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error)

// encode columns for the "key" of the KV
// func encodeKey(out []byte, prefix uint32, vals []Value) []byte
// decode columns from the "value" of the KV
func decodeValues(in []byte, out []Value)

func (tree *BTree) Update(req *UpdateReq)

func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	req := UpdateReq{Key: key, Val: val, Mode: mode}
	if _, err = db.kv.Update(&req); err != nil {
		return false, err
	}
	// maintain secondary indexes
	if req.Updated && !req.Added {
		// use `req.Old` to delete the old indexed keys ...
	}
	if req.Updated {
		// add the new indexed keys ...
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.Pkeys])
	val := encodeValues(nil, values[tdef.Pkeys:])

	// TODO (FIX THIS)
	return db.kv.Update(key, val, mode)
	return req.Updated, nil
}


// TODO
func (db *DB) TableNew(tdef *TableDef) error

// func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
// 	// ...
// 	// insert the row
// 	req := UpdateReq{Key: key, Val: val, Mode: mode}
// 	if _, err = db.kv.Update(&req); err != nil {
// 		return false, err
// 	}
// 	// maintain secondary indexes
// 	if req.Updated && !req.Added {
// 		// use `req.Old` to delete the old indexed keys ...
// 	}
// 	if req.Updated {
// 		// add the new indexed keys ...
// 	}
// 	return req.Updated, nil
// }
