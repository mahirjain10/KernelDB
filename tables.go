package main

// Instead of storing multiple tables in multiple btree
// We are going to insert all the tables in a single btree
// Now to segregate which data belongs to which tables
// we add a prefix to the key (which includes column data like an ID),
// and this key maps to the values (the row data).

const (
	TYPE_BYTES = 1 // any length / aribitrary length of string
	TYPE_INT64 = 2 // integer 
)

type DB struct {
	Path string
	kv KV
	}
// table cell

type Value struct{ 
	Type uint32 // tagged uinon
	I64 int64 // stores int64
	Str []byte // stores string
}

// IMP : A tagged union is a way to store multiple possible data types in one place, with a "tag" to indicate which type is active.

// table row 
type Record struct{ 
	Cols []string  // list column names
	Vals []Value   // list of values 
}


// add a string to a row


// column name =["name","email"]
// value = [{"mahir,"mahir@gmail.com},{"neel,neel@gmail.com"}]
func (rec *Record) AddStr(col string,val []byte) *Record{
	// add column name to the row
	rec.Cols = append(rec.Cols, col)
	// add string value to the row
	rec.Vals = append(rec.Vals, Value{Type: 1,Str: val})
	return rec
}

// 
func (rec *Record) AddInt64(col string, val int64) *Record
func (rec *Record) Get(col string) *Value


// Schema design

type TableDef struct{
	// user defined
	Name string
	Types []uint32 // column types (in our case int or string)
	Cols []string // column names
	Pkeys int  // tells which column is / are primary keys from start
	// primary key can be 0 that mean no primary key
	// ["id","email","name"] = id and email are primary key ,if Pkeys = 2
	// auto-assigned B-tree key prefixes for different tables
	Prefix uint32
}

//  predefined internal tabe
var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name: "@table",
	Types: []uint32{TYPE_BYTES,TYPE_BYTES},
	Cols: []string{"name","def"},
	Pkeys: 1,
}

var TDEF_META = &TableDef{
	Prefix: 1,
	Name: "@meta",
	Types: []uint32{TYPE_BYTES,TYPE_BYTES},
	Cols: []string{"key","val"},
	Pkeys: 1,
}

func (db *DB) Get(table string, rec *Record) (bool, error){
	
}
func (db *DB) Insert(table string, rec Record) (bool, error)
func (db *DB) Update(table string, rec Record) (bool, error)
func (db *DB) Upsert(table string, rec Record) (bool, error)
func (db *DB) Delete(table string, rec Record) (bool, error)