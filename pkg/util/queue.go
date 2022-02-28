package util

// Queue implements a doubly linked list with a fixed size.
// This is not safe for access by multiple goroutine.
type Queue struct {
	head *node
	tail *node

	length    int
	maxLength int
}

// NewQueue create a Queue with specified maxLength
func NewQueue(maxLength int) *Queue {
	return &Queue{
		length:    0,
		maxLength: maxLength,
	}
}

type node struct {
	next     *node
	previous *node

	payload interface{}
}

// Peek returns the tail of the queue but does not remove it
func (ll *Queue) Peek() interface{} {
	if ll.tail == nil {
		return nil
	}

	return ll.tail.payload
}

// Poll removes the last element in the list
func (ll *Queue) Poll() interface{} {
	var (
		oldTail *node
		newTail *node
	)

	oldTail = ll.tail

	if oldTail == nil {
		return nil
	}

	ll.length--
	newTail = oldTail.previous

	// queue is now empty
	if newTail == nil {
		ll.tail = nil
		ll.head = nil
		return oldTail.payload
	}

	newTail.next = nil
	ll.tail = newTail

	return oldTail.payload
}

// Push adds a new element to the head of the list
// after the element is added the list is trimmed
func (ll *Queue) Push(payload interface{}) {
	var (
		newHead *node
		oldHead *node
	)

	newHead = &node{payload: payload}
	ll.length++

	// first node
	if ll.head == nil {
		ll.head = newHead
		ll.tail = newHead
		return
	}

	oldHead = ll.head
	newHead.next = oldHead
	oldHead.previous = newHead
	ll.head = newHead

	ll.trim()
}

// Length get the current length of the list
func (ll *Queue) Length() int {
	return ll.length
}

func (ll *Queue) trim() {
	if ll.length <= ll.maxLength {
		return
	}

	nodesToTrim := ll.length - ll.maxLength
	for i := 0; i < nodesToTrim; i++ {
		ll.tail = ll.tail.previous
		ll.tail.next = nil
		ll.length--
	}
}
