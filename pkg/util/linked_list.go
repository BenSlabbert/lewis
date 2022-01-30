package util

import "sync"

type LinkedList struct {
	head *node
	tail *node

	length    int
	maxLength int

	mtx sync.Mutex
}

func NewLinkedList(maxLength int) *LinkedList {
	return &LinkedList{
		length:    0,
		maxLength: maxLength,
		mtx:       sync.Mutex{},
	}
}

type node struct {
	next     *node
	previous *node

	payload interface{}
}

func (ll *LinkedList) Add(payload interface{}) {
	ll.mtx.Lock()
	defer ll.mtx.Unlock()

	newHead := &node{
		payload: payload,
	}
	ll.length++

	// first node
	if ll.head == nil {
		ll.head = newHead
		ll.tail = newHead
		return
	}

	newHead.previous = ll.head
	ll.head.next = newHead
	ll.head = newHead
}

func (ll *LinkedList) Length() int {
	return ll.length
}

func (ll *LinkedList) Trim() int {
	if ll.length <= ll.maxLength {
		return 0
	}

	ll.mtx.Lock()
	defer ll.mtx.Unlock()

	nodesToTrim := ll.length - ll.maxLength
	for i := 0; i < nodesToTrim; i++ {
		ll.tail = ll.tail.next
		ll.tail.previous = nil
		ll.length--
	}

	return nodesToTrim
}
