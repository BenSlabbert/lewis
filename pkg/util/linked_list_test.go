package util

import (
	"sync"
	"testing"
)

func TestLinkedList_Add(t *testing.T) {
	ll := LinkedList{
		maxLength: 10,
		mtx:       sync.Mutex{},
	}

	ll.Add(1)
	ll.Add(2)
	ll.Add(3)

	if ll.head.payload != 3 {
		t.Fatal()
	}

	if ll.tail.payload != 1 {
		t.Fatal()
	}

	previous := ll.head.previous
	if previous.payload != 2 {
		t.Fatal()
	}

	previous = previous.previous
	if previous.payload != 1 {
		t.Fatal()
	}

	if previous.previous != nil {
		t.Fatal()
	}
}

func TestLinkedList_Trim(t *testing.T) {
	ll := LinkedList{
		maxLength: 3,
		mtx:       sync.Mutex{},
	}

	ll.Add(1)
	ll.Add(2)
	ll.Add(3)
	ll.Add(4)
	ll.Add(5)

	trim := ll.Trim()
	if trim != 2 {
		t.Fatal()
	}

	if ll.Length() != 3 {
		t.Fatal()
	}

	if ll.tail.next == nil {
		t.Fatal()
	}

	if ll.tail.previous != nil {
		t.Fatal()
	}

	if ll.tail.payload != 3 {
		t.Fatal()
	}
}

func TestLinkedList_TrimNotNeeded(t *testing.T) {
	ll := LinkedList{
		maxLength: 5,
	}

	ll.Add(1)
	ll.Add(2)
	ll.Add(3)
	ll.Add(4)
	ll.Add(5)

	trim := ll.Trim()
	if trim != 0 {
		t.Fatal()
	}

	if ll.Length() != 5 {
		t.Fatal()
	}
}
