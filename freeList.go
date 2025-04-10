package main

// Data is stored in pages, which are linked together.
// When a page is deleted, it is not removed but added to a free list.
// The free list is a separate structure that tracks reusable pages.
// When new data arrives, the DB fetches a page from the free list and writes data into it.
// This prevents unnecessary disk allocations and improves storage efficiency.
// PS : The data on stored on pages would not be continous

// alias for byte
type LNode []byte

// header of LNode
const FREE_LIST_HEADER = 8

// how many uint64 can be stored
// eg 4096 (size of a node of Btree)
// 4096-8/8 = 511 uint64
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

func (node LNode) getNext() uint64
func (node LNode) setNext(next uint64)
func (node LNode) getPtr(index int) uint64

// ptr here isnt pointer to memory as we arent storing data in memory but in disk
// so its a pointer in form of numbers
func (node LNode) setPtr(index int, ptr uint64)

type FreeList struct {
	// callbacks for managing on disk-pages
	get func(uint64) []byte // read a page
	new func([]byte) uint64 //append a new page
	set func(uint64) []byte // update an existing page
	// persisted data in the meta page
	headPage uint64 // pointer to the list head node
	headSeq  uint64 // monotonic sequence number to index into the list head
	tailPage uint64
	tailSeq  uint64
	// in-memory states
	maxSeq uint64 // saved `tailSeq` to prevent consuming newly added items
	maxVer uint64 // the oldest reader version
	curVer uint64 // version number when commiting
}

// get 1 item from the list head.return 0 on failure
// func (fl *FreeList) PopHead() uint64

// // add 1 item to the tail
// func (fl *FreeList) PushTail(ptr uint64)

// to map sqeuence to index
func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

// step 1 : At the beginning of the update, save the original tailSeq to maxSeq (maxSeq acts as fence or boundary)
// step 2 : During the update, headSeq cannot overrun maxSeq (if headSeq overruns the boundary it would lead to incosistency ,thats why we limit headSeq traversal till fences during update)
// step 3 : At the beginning of the next update, maxSeq is advanced to tailSeq

// make the newly added items available for consumption
func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

//                   FREE LIST STRUCTURE
//  STORE PAGES AND NEXT POINTERS in a NODE
// +------------------+      +------------------+      +------------------+
// |  headPage (N1)  | -->  |  node (N2)       | -->  |  node (N3)       | --> NULL
// |-----------------|      |-----------------|      |-----------------|
// |  Free Items:    |      |  Free Items:    |      |  Free Items:    |
// |  [P1, P2, P3]   |      |  [P4, P5, P6]   |      |  [P7, P8, P9]   |
// |  next -> N2     |      |  next -> N3     |      |  next -> NULL   |
// +------------------+      +------------------+      +------------------+

// remove an item from head node
// if node gets empty move onto the next node
// if it reaches the last node ,assert ensures the list alaways has at least one node
// if there is no last node as a place holder then it would be in bad state , i.e the head would point to null
// if it points to null ,you wont able to insert free pages

//  returns ptr to disk being removed
// returns previous head
func flPop(fl *FreeList) (ptr uint64, head uint64) {

	// headPage is a node itself 
	// headSeq is the index which keep tracks of item inside a node
	// when headSeq reaches the reset the index to 0 
	// this means we poped out all the items in a node
	// there is no element to pop
	if fl.headSeq == fl.maxSeq {
		return 0, 0 // no more free items
	}
	node := LNode(fl.get(fl.headPage)) // get the current node
	ptr = node.getPtr(seq2idx(fl.headPage)) // get the item ,item from the node
	fl.headSeq++                            // increment head seq to move to next item

	// mIf we used up all items in this node, move to the next one
	if seq2idx(fl.headSeq) == 0 {
		head, fl.headPage = fl.headPage, node.getNext() // Move to the next node
		assert(fl.headPage != 0)
	}

	// we arent deleting the node which is emptied (all the items consumed or deleted)
	// instead of allocating a new node, the system can reuse 
	return // this is called implicit return in go
	// it automatically returns the values
}

func (fl *FreeList) PopHead()uint64{
	ptr ,head := flPop(fl);
	if head !=0{
		fl.PushTail(head)
	}
	return ptr
}

func (fl *FreeList) PushTail(ptr uint64){
	
	// add it to the tail node
	// fl.set = set a new page
	// set the pointer of tail node 
	// seq2idx = ccalculates the index of an item within a specific LNode, not the index of the node itself in the free list.
	LNode(fl.set(fl.tailPage)).setPtr(seq2idx(fl.tailSeq),ptr)
	fl.tailSeq++;
	
	//add a new tail node if its full(list is neevr empty)
	// create a new tail node
	if seq2idx(fl.tailSeq)==0{
		next ,head := flPop(fl) //may remove head
		if next==0{
			// allocate a new node by appending
			next = fl.new(make([]byte, BTREE_PAGE_SIZE))
		}

		// link to new tail node
		// This step links the old tail to the new node and updates the tail pointer.
		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage=next

		// also add the head node if its removed
		if head!=0{
			LNode(fl.set(fl.tailPage)).setPtr(0,head)
			fl.tailSeq++
		}
	}
}