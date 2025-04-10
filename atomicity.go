package main

const (
	FLAG_DELETED = byte(1)
	FLAG_UPDATED = byte(2)
)

// start <= key <= stop
type KeyRange struct {
	// for single key read ,the start and stop are equal
	start []byte // starting point to read or write
	stop []byte // ending point to read or write
}
type KVTX struct {
	db       *KV    // reference to the key value of db
	meta     []byte // meta data used for the rollback
	snapshot BTree  // read only state . points to root of the tree ,snapshot of the db before tx begins
	pending  BTree  // pending state changes in memory ,they are local changes // it is a btree itself
	version  uint64 // based on KV.version
	read []KeyRange
}
type DBTX struct {
	kv KVTX
	db *DB
}

// an iterator that combines pending updates and the snapshot
type CombinedIter struct {
	top *BIter // KVTX.pending
	bot *BIter // KVTX.snapshot

}

// begin a transaction
func (kv *KV) Begin(tx *KVTX) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	// read-only snapshot,just tree root and the pages read callback
	tx.snapshot.root = kv.tree.root // snapshot of root to revert back to the state before changes
		chunks := kv.mmap.chunks // copied to avoid updates from writers
	tx.snapshot.get = func(u uint64) []byte {return mmapRead(ptr,chunks)}//............ rread from mmaped pages
	pages := [][]byte(nil)                                           // A slice to store in-memory B+tree nodes // read pending changes
	tx.pending.get = func(ptr uint64) []byte { return pages[ptr-1] } // retrieve the pages from pointer
	tx.pending.new = func(node []byte) uint64 {                      // add new pending data
		pages = append(pages, node) // add node to pages
		return uint64(len(pages))   // rteurn pointers
	}
	tx.pending.del = func(uint64) {}
	// tx.db = kv                // store the refernce of the actual database
	// tx.meta = saveMeta(tx.db) // saves the current metadata state of the db
}

// READ BACK YOUR OWN WRITE (WHICH MEANS WHATEVER CHANGES YOU MADE SHOULD BE SEEN INSTANTLY TO YOU)
func (tx *KVTX) Get(key []byte) ([]byte, bool) {
	val, ok := tx.pending.get(key)
	switch {
	case ok && val[0] == FLAG_UPDATED:
		return val[1:], true
	case ok && val[0] == FLAG_DELETED:
		return nil, false
	case !ok: // not in pending, check snapshot
		return tx.snapshot.get(uint64(key[]));
	default :
	panic("unreachable")
	}
}

// end a transaction: commit updates; rollback on error
func (kv *KV) Commit(tx *KVTX) error {
	// return updateOrRevert(tx.db, tx.meta) //update or revert the changes ,if there are errors
	if len(writes) > 0 {
		kv.history = append(kv.history, CommittedTX{kv.version, writes})
	}
		return nil
}

func detectConflicts(kv *KV,tx *KVTX) bool{
	for i := len(kv.history) -1 ;i>= 0;i--{
		if !versionBefore(tx.version,kv.history[i].version){
			break; // sorted
		}
		if rangeOverlap(tx.reads,kv.history[i],writes){
			return true
		}
	}
	return false
}

// end a transaction: rollback
func (kv *KV) Abort(tx *KVTX) {
	loadMeta(tx.db, tx.meta)                 // revert the db to old state
	tx.db.page.nappend = 0                   // discard append writes
	tx.db.page.updates = map[uint64][]byte{} //clear temporary updates
}

// Transaction (TX): A set of database operations (reads/writes) that either all happen or all undo, keeping data safe.
// Isolation Level: How a transaction sees changes from others. Snapshot isolation gives it a "frozen view" of the database.
// B+tree: A tree structure that organizes data for fast searches and updates.
// Copy-on-Write: Changes are made on a copy, not the original, so the snapshot stays untouched.
// Snapshot: A read-only "photo" of the database at the transaction’s start, using the B+tree’s root.
// Local Updates: Changes (writes/deletes) stored in a separate in-memory B+tree (pending) until the transaction commits.