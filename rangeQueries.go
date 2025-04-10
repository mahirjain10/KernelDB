package main

import (
	"encoding/binary"
	"slices"
)

const (
	CMP_GE = +3 // >=
	CMP_GT = +2 // >
	CMP_LT = -2 // <
	CMP_LE = -3 // <=
)

type Scanner struct {
	// range ,from key1 to key2
	Cmp1 int
	Cmp2 int
	Key1 Record
	Key2 Record
}
type BIter struct {
	tree *BTree
	path []BNode  // from root to leaf
	pos  []uint16 //pos of nodes along with path

}
type RecordIter interface {
	Valid() bool
	Next()
	Deref(*Record) error
}

// find the closest pos that is less or equal to the input key
func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := tree.get(ptr)
		index := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, index)
		ptr = node.getPtr(index)
	}
}

// get the current KV pair

func (iter *BIter) Deref() ([]byte, []byte)

// pre  condition of Deref()

func (iter *BIter) Valid() bool

func (tx *KVTX) Update(req *UpdateReq) bool {
	return tx.db.tree.Update(req)
}
func (tx *KVTX) Del(req *DeleteReq) bool {
	return tx.db.tree.Delete(req)
}

// moving backward and forward

func (iter *BIter) Prev()
func (iter *BIter) Next() {
	iterNext(iter, len(iter.path)-1)
}

func iterNext(iter *BIter, level int) {
	if iter.pos[level]+1 < iter.path[level].nkeys() {
		iter.pos[level]++ // iterate within node
	} else if level > 0 { // if level is greater than 0 ,i.e root => 0, not root
		// as of now we havent linked leaf nodes as doubly linked list so we need to backtrack to parent and then to sibings
		iterNext(iter, level-1) // lets backtrack to parent node
	} else {
		// there is no next key to iterate ,return back
		iter.pos[len(iter.pos)-1]++
		return
	}

	if level+1 < len(iter.pos) {
		node := iter.path[level]
		kid := BNode(iter.tree.get(node.getPtr(iter.pos[level])))
		iter.path[level+1] = kid
		iter.pos[level+1] = 0
	}
}

func (tx *KVTX) Seek(key []byte, cmp int) *BIter {
	return tx.db.tree.Seek(key, cmp)
}

// within the range or not?
func (sc *Scanner) Valid() bool

// move the underlying B-tree iterator
func (sc *Scanner) Next()

// fetch the current row
func (sc *Scanner) Deref(rec *Record)
func (tx *DBTX) Scan(table string, req *Scanner) error

func dbScan(db *DB, tdef *TableDef, req *Scanner) error {
	// ...
	covered := func(key []string, index []string) bool {
		return len(index) >= len(key) && slices.Equal(index[:len(key)], key)
	}
	req.index = slices.IndexFunc(tdef.Indexes, func(index []string) bool {
		return covered(req.Key1.Cols, index) && covered(req.Key2.Cols, index)
	})

	// ...
}

// order preserving encoding

func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		out = append(out, byte(v.Type)) // doesnt tsrat with 0xff
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)        //flip the sign bit,So that negative numbers sort before positive ones when comparing byte-wise
			binary.BigEndian.PutUint64(buf[:], u) //big indian
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null terminated
		default:
			panic("what ???") //for unknow type
		}
	}
	return out
}

// for primary keys and indexes
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {

	// 4-byte table prefix

	var buf [4]byte
	//  [:] use whole slice
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)

	// order-preserving encoded keys
	out = encodeValues(out, vals)
	return out
}

// for the input range ,which can be a prefix of the key

func encodeKeyPartial(out []byte, prefix uint32, vals []Value, cmp int) []byte {
	out = encodeKey(out, prefix, vals)
	// encode missing columns as infinity
	if cmp == CMP_GT || cmp == CMP_LT {
		// unreachable +infinity
		out = append(out, 0xff)
	} // else: -infinity is the empty string
	return out
}
type qlSelectIter struct {
	iter RecordIter // input
	names []string
	exprs []QLNode
	}
	func (iter *qlSelectIter) Valid() bool {
	return iter.iter.Valid()
	}
	func (iter *qlSelectIter) Next() {
	iter.iter.Next()
	}
	func (iter *qlSelectIter) Deref(rec *Record) error {
	if err := iter.iter.Deref(rec); err != nil {
	return err
	}
	vals, err := qlEvelMulti(*rec, iter.exprs)
	if err != nil {
		return err
		}
		*rec = Record{iter.names, vals}
		return nil
		}
		