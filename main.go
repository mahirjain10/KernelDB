package main

import (
	"bytes"
	"encoding/binary"
)

// 1KB = 4096 bytes
// HEADER is used to store meta data about a node
const HEADER = 4 // 4 BYTES

// BTREE PAGE SIZE = OS page size
const BTREE_PAGE_SIZE = 4096 // i.e 4KB

// BTREE MAX KEY SIZE
const BTREE_MAX_KEY_SIZE = 1000 // i.e 1000 Bytes

// BTREE MAX KEY SIZE
const BTREE_MAX_VALUE_SIZE = 3000 // i.e 3000 Bytes

type BNode []byte // can be dumped to the disk
type BTree struct {
	root uint64 // pointer (a non zero page number)

	// callbacks for managing on-disk pages

	// reads a page from disk
	get func(uint64) []byte // derefernce a pointer

	// allocates and write a new page (copy-on-write)
	new func([]byte) uint64 // allocate a new page
	del func(uint64)        // deallocate a page

}

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VALUE_SIZE
	assert(node1max <= BTREE_PAGE_SIZE)
}

// type of node
// nkeys = no of keys in a node
// pointers = array of pointers where each pointers if of 8 bytes || pointers points to child and its an integer and Pointers store the disk offset (byte position) of a child node.
// offsets = array of pointers pointing to position of key - value postion
// klen and vlen are important as they are used to jump from one keys to different keys
// Internal node: ptrs[] contains disk offsets of child nodes.
// Leaf node: offsets[] contains disk offsets of key-value pairs.

//  | type | nkeys | pointers | offsets | key-values | unused |
// | 2B | 2B | nkeys * 8B | nkeys * 2B | ... |              |
//
// | klen | vlen | key | val |
// | 2B | 2B | ... | ... |

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

// HEADER

// BigEndian and LittleEndian is used to encode and decode binary data
// BigEndian for nkeys and btype → Ensures platform independence for metadata.
// LittleEndian for getPtr → Optimized for memory access in modern CPUs.

// return type of node
func (node BNode) btype() uint64 {
	return binary.BigEndian.Uint64(node[0:2])
}

// return number of keys
func (node BNode) nkeys() uint16 {
	return binary.BigEndian.Uint16(node[2:4])
}

// set header
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// retrieve the CHILD using POINTER
func (node BNode) getPtr(index uint16) uint64 {
	assert(index < node.nkeys()) //
	pos := HEADER + 8*index
	return binary.LittleEndian.Uint64(node[pos:])
}

// offset tells the position of key and value pair

// Offset list

// get offset starting position from the memory layout
func (node BNode) offsetPos(index uint16) uint16 {
	assert(1 <= index && index <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(index-1)
}

// function to get chunk of offset data , i.e 2bytes by giving it the starting position
func (node BNode) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, index):])
}

// set offset
func (node BNode) setOffset(index uint16, offset uint16)

// get the starting postion
func (node BNode) kvPos(index uint16) uint16 {
	assert(index <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(index)
}

// get the key ,skip 4 bytes and then procceed
func (node BNode) getKey(index uint16) []byte {
	assert(index <= node.nkeys())
	pos := node.kvPos(index)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(index int16) []byte

func (node BNode) nbytes() uint16 {
	return node.kvPos(uint16(node.nkeys()))
}

// KV lookups within a node

// returns the first kid node whose range intersects the key

func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0) // 0 is not found

	// the first key is a copy from parent node
	// thus it is always less than or equal to the key

	// many BTrees has copy of parent node in child as a key to keep the search bounded and help for splitting and merging and makes insetion and deletion easier
	//  [50]
	// 	/	\
	// [20,30] [50*,60]

	// B+ tree keeps copy of internal nodes for structured lookup

	// 		[5,20]
	//     /    \     \
	//    [2]=>[5,10]=> [20,30]

	//  starting the lookup from first key as the 0th is key is copy of the parent node ,as show in above example
	for i := uint16(1); i < nkeys; i++ {
		compare := bytes.Compare(node.getKey(i), key)
		// we want to insert the key in sorted order , so we need to find key which is less than or equal to new key
		if compare <= 0 {
			found = i
		}
		if compare >= 0 {
			break
		}
	}
	return found
}

// Add a new key to leaf node

func leafInsert(new BNode, old BNode, index uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nbytes()+1) // setting up header
	nodeAppendRange(new, old, 0, 0, index)    // copy the keys and values before the index

	// insert the key in the new node
	nodeAppendKv(new, index, 0, key, val)

	//
	nodeAppendRange(new, old, index+1, index, old.nkeys()-index)
}

// Copy a KV into the position

func nodeAppendKv(new BNode, index uint16, ptr uint64, key []byte, val []byte) {

	// ptrs
	new.setPtr(index, ptr) // store the ptr

	// KVs

	pos := new.kvPos(index) // calculate the position for KV storage

	// store the length of the key in first 2 byte
	binary.LittleEndian.AppendUint16(new[pos+0:], uint16(len(key)))

	// store the length of the value in next 2 bytes
	binary.LittleEndian.AppendUint16(new[pos+2:], uint16(len(val)))

	// copy the old key and value in new node
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)

	// the offset of the next key
	new.setOffset(index+1, new.getOffset(index)+4+uint16((len(key)+len(val))))

}

func nodeAppendRange(new BNode, old BNode, newDst uint16, oldSrc uint16, n uint16) {}

func nodeReplaceKidN(tree *BTree, new BNode, old BNode, index uint16, kids ...BNode) {
	noOfKids := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+noOfKids-1)
	for i, node := range kids {
		nodeAppendKv(new, index+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, index+noOfKids, index+1, old.nkeys()-(index+1))
}

// Split a oversized node into 2 so that the 2nd node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {}

// Split a node into 2 ,if its big split it into 3
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		// we know the node size is less than page size
		// we are truncating to ensure there is no inconsistency
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not splitting
	}
	// Note that the returned nodes are allocated from memory; they are
	// just temporary data until nodeReplaceKidN actually allocates them.
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) //  left node would be 2X size of page node so we can split into 2 nodes
	right := BNode(make([]byte, BTREE_PAGE_SIZE))  // right node would be of size of page to ensure data fits

	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		// we know the node size is less than page size
		// we are truncating to ensure there is no inconsistency
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // returning two nodes
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{left, middle, right} // returning three nodes
}

// insert a KV into a node, the result might be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {

	// the result node
	// it is allowed to be bigger than 1 page size and will be split if so
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// where to insert the key ???
	// find the key which should be <= new key
	index := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		// if the node is leaf node and the key is present in th node
		if bytes.Equal(key, node.getKey(index)) {
			//  found the existing key and update the exisitng value
			leafUpdate(new, node, index, key, val)
		} else {
			// insert the new key if no existing key is found
			leafInsert(new, node, index+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		nodeInsert(tree, new, node, index, key, val)

	default:
		panic("bad node!!")
	}
	return new
}

// part of the treeInsert() : KV insertion to an internal node

func nodeInsert(tree *BTree, new BNode, node BNode, index uint16, key []byte, val []byte) {
	kptr := node.getPtr(index)

	// recursive insertion to the kid node
	knode := treeInsert(tree, node, key, val)

	// split the result
	nsplit, split := nodeSplit3(knode)

	// deallocate the old kid node
	tree.del(kptr)

	// update the the kid links
	nodeReplaceKidN(tree, new, node, index, split[:nsplit]...)
}

func (tree *BTree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		// Edge Case Problem: If the tree is empty, lookup (nodeLookupLE) may fail for very small keys.
		// Fix: A sentinel value (empty key nil) is inserted at index 0.
		// Result: Lookups will always find a valid key position, even for the smallest possible key.
		// General Sentinel values help avoid special-case logic and simplify tree operations
		// create the first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)

		// a dummy key ,this make the tree cover the whole key space

		// creating a sentinal node to help us inserting a key value pair because
		//  we need to compare the key value and insert
		nodeAppendKv(root, 0, 0, nil, nil)
		nodeAppendKv(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}
	//  finds the correct place to insert
	node := treeInsert(tree, tree.get(tree.root), key, val)

	//  ensure node doesnt overflow
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)

	// create a new level
	if nsplit > 1 {
		// the root was split and add a new level
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKv(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

// Node update functions for tree deleting

// remove a key from a leaf node
func leadDelete(new BNode, old BNode, index uint16)

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode)

// replace 2 adjacent links with 1
func nodeReplace2Kid(new BNode, old BNode, index uint16, ptr uint64, key []byte)

func shouldMerge(tree *BTree, node BNode, index uint16, updated BNode) (int, BNode) {

	// if updated node is greater than 25% threshold , dont merge
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	// if updated node is less than 25% threshold then find where to merge
	if index > 0 {
		// get the left sibling node with the help of ptr
		sibling := BNode(tree.get(node.getPtr(index - 1)))

		// sibling's total bytes + updated total bytes - a header size (merging two nodes would have two headers and we want one header of a node)
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling //left
		}
	}

	// Check if the right node exists next to updated node
	if index+1 < node.nkeys() {

		// get the right sibling node with the help of ptr
		sibling := BNode(tree.get(node.getPtr(index + 1)))
		// sibling's total bytes + updated total bytes - a header size (merging two nodes would have two headers and we want one header of a node)
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}
	return 0, BNode{}
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode

// delete a key from internal node ; part of the treeDelete()
func nodeDelete(tree *BTree, node BNode, index uint16, key []byte) BNode {

	//recurse into the kid
	kptr := node.getPtr(index) // get the child node
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{}
	}
	tree.del(kptr) // delete a node

	new := BNode(make([]byte, BTREE_PAGE_SIZE))

	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, index, updated)
	switch {
	case mergeDir < 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(index - 1))
		nodeReplace2Kid(new, node, index-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(index + 1))
		nodeReplace2Kid(new, node, index, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0: // no valid left or right sibling to merge with and child node became empty after deletion
		assert(node.nkeys() == 1 && index == 0) // 1 empty child but no sibling
		new.setHeader(BNODE_NODE, 0)          // the parent becomes empty too
	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, index, updated)
	}
	return new
}

