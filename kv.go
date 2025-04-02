package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"syscall"

	"firebase.google.com/go/db"
	"golang.org/x/sys/unix"
)

const DB_SIG = "BuildYourOwnDB06" // db signature 
// -------DB METADATA----------------
// | sig | root_ptr | page_used |
// | 16B | 8B | 8B |



type KV struct {
	Path string // file name

	// internals
	fd   int // file descriptor
	tree BTree
	mmap struct {
		total int // mmap can be larger than page size
		chunks [][] //multiple mmap ,can be non-contagious
	}
	page struct {
		flushed uint64 // database size in number of pages
		temp [][]byte // newly allocated pages
	}
	failed bool // did the last update fail?
	free FreeList 
}

func (db *KV) Open() error
func (db *KV) Get(key []byte) ([]byte, error) {
	return db.tree.Get(uint64(key[0])), nil
}

func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return updateFile(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted :=
		db.tree.Delete(key)
	return deleted, updateFile(db)
}

func updateFile(db *KV) error {
	// 1. Write new nodes
	if err := writePages(db); err != nil {
		return err
	}

	// 2. `Fsync` to enforce the order between 1 and 3
	if err := syscall.Fsync(db.fd); err !=nil{
		return err
	}

	// 3. Update the root pointer atomically
	if err := updateRoot(db); err != nil{
		return err
	}

	// 4. `Fsync` to make everything persistent
	return syscall.Fsync(db.fd)
}


func createFileSync(file string) (int, error) {
    // Obtain the directory fd
    flags := os.O_RDONLY | syscall.O_DIRECTORY
    direfd, err := syscall.Open(path.Dir(file), flags, 0o644)
    if err != nil {
        return -1, fmt.Errorf("open directory: %w", err)
    }
    defer syscall.Close(direfd)

    // Open or create the file
    flags = os.O_RDWR | os.O_CREATE
    fd, err := unix.Openat(direfd, path.Base(file), flags, 0o644)
    if err != nil {
        return -1, fmt.Errorf("open file: %w", err)
    }

    // Sync the directory to ensure the file creation is persisted
    if err := syscall.Fsync(direfd); err != nil {
        syscall.Close(fd)
        return -1, fmt.Errorf("fsync directory: %w", err)
    }

    return fd, nil
}


// saving pages into disk ,swapping them into ram when required
func Mmap(fd int,offset int64,length int)(data []byte,err error)


func(db *KV) Open() error{
	db.tree.get = db.pageRead // read a page
	// db.tree.new = db.pageAppend // append a page
	// db.tree.del={}
	db.tree.new = db.pageAlloc // (new) reuse from the free list or append
db.tree.del = db.free.PushTail // (new) freed pages go to the free list
// free list callbacks
db.free.get = db.pageRead // read a page
db.free.new = db.pageAppend // append a page
db.free.set = db.pageWrite // (new) in-place updates
}
// fd = File descriptors allow programs to perform operations like reading, writing, or closing the file without needing to know the underlying details of the resource.
// offset = from where to start reading
// PROT_READ = means the mapped memory can be read but not necessarily written to.
// MAP_SHARED = 	means changes to the mapped memory are shared with other processes that map the same file.
syscall.Mmap(fd,offset,size,syscall.PROT_READ,syscall.MAP_SHARED)


func (db *KV) pageRead(ptr uint64)[]byte{
	// mmap chunks have local index , so start is used to find the chunk from local start to end
	start := uint64(0)
	for _ , chunk = range db.mmap.chunks{
		end = start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end{
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset:offset*BTREE_PAGE_SIZE]
		}
		start=end
	}
	panic("bad ptr")
}


// increase the size of mmap

func extendedMmap(db *KV,size int)error{
	if size <= db.mmap.total{
		return nil // we have enough space
	}

	// check if the total of mmap is greater or 64 
	// if total is greater ,intialize alloc with it else with 64 MB
	alloc := max(db.mmap.total,64<<20)

	for db.mmap.total + alloc <size{
		alloc * 2 // if total + alloc isnt equal to size we need,then double it
	}

// 	int64(db.mmap.total): Offset (where new memory mapping starts).
// alloc: Size of the new mapping.
// syscall.PROT_READ: Read-only access.
// syscall.MAP_SHARED: Changes are shared with other processes.
	chunk err := syscall.Mmap(db.fd,int64(db.mmap.total),alloc,syscall.PROT_READ,syscall.MAP_SHARED)
	if err !=nil{
		return fmt.Errorf("mmap : %w",err)
	}

	//update the meta data
	db.mmap.total += alloc

	// append the chumks to extending chunks
	db.mmap.chunks = append(db.mmap.chunks, chunk)

	return nil
}


func (db *KV) pageAppend(node []byte) uint64 {
    // Calculate the new page number:
    // - `db.page.flushed`: Number of pages already written to disk.
    // - `len(db.page.temp)`: Number of pages currently in temporary memory.
    ptr := db.page.flushed + uint64(len(db.page.temp))

    // Store the new page in temporary memory (not yet written to disk).
    db.page.temp = append(db.page.temp, node)

    // Return the assigned page number (offset).
    return ptr
}

func writePages(db *KV) error{
	// extend MMap if required
	// `size` : size required to extend the MMap
	// already flushed page size + pages stored in temp * 4KB (page size)
	size := (int(db.page.flushed)+len(db.page.temp))*BTREE_PAGE_SIZE 

	if err := extendedMmap(db,size); err != nil{
		return err
	}

	// write date pages into the file

	// position from we need to write
	offset := int64(db.page.flushed *BTREE_PAGE_SIZE)
	// write to file permanently
	// `db.fd` : file descriptor 
	// `db.page.temp` : pages store in temp memory
	// `offset` : from where to start writing
	if _,err := unix.Pwritev(db.fd,db.page.temp,offset);err!=nil{
		return err
	}

	// discard in-memory data

	// increasing the size of flushed
	db.page.flushed += uint64(len(db.page.temp))
	// emptying temp array of bytes 
	// resetting the byte array without deleting it
	// slice length = 0
	db.page.temp = db.page.temp[:0]
	return nil
}

func saveMeta(db *KV) []byte{
	// meta data is of 32 bytes
	var data[32] byte
	copy(data[:16],[]byte(DB_SIG))
	binary.LittleEndian.AppendUint64(data[16:],db.tree.root)
	binary.LittleEndian.AppendUint64(data[24:],db.page.flushed)
	return data[:]
}

func loadMeta(db *KV, data []byte)

func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 { // empty file
	db.page.flushed = 1 // the meta page is initialized on the 1st write
	return nil
	}
	// read the page
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	// verify the page
	// ...
	return nil
}
	
func updateRoot(db *KV) error{
	if _,err := syscall.Pwrite(db.fd,saveMeta(db),0);err!=nil{
		return fmt.Errorf("write meta page :%w",err)
	}
	return nil
}

func (db *KV) Set(key []byte,val []byte) error{
	meta := saveMeta(db)
	db.tree.Insert(key,val)
	return updateOrRevert(db,meta)
}

func updateOrRevert(db *KV,meta []byte)error{
	//2 phase update
	err := updateFile(db)
	// revert on error 
	if err != nil{
		// the in-memory states can be reverted immediately to allow reads
		loadMeta(db,meta)
		// discard temporaries
		db.page.temp=db.page.temp[:0]

	}

	if db.failed{
		db.failed=false
	}
	return err
}