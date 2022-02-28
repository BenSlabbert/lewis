package util

import (
	"testing"
)

func TestQueue(t *testing.T) {
	queue := NewQueue(10)
	queue.Push(1)
	queue.Push(2)
	queue.Push(3)

	if queue.head.payload != 3 {
		t.Fatal()
	}

	if queue.tail.payload != 1 {
		t.Fatal()
	}

	next := queue.head.next
	if next.payload != 2 {
		t.Fatal()
	}

	next = next.next
	if next.payload != 1 {
		t.Fatal()
	}

	if next.next != nil {
		t.Fatal()
	}
}

func TestQueue_Trim(t *testing.T) {
	queue := NewQueue(3)
	queue.Push(1)
	queue.Push(2)
	queue.Push(3)
	queue.Push(4)
	queue.Push(5)

	if queue.Length() != 3 {
		t.Fatal()
	}

	if queue.tail.next != nil {
		t.Fatal()
	}

	if queue.tail.previous == nil {
		t.Fatal()
	}

	if queue.tail.payload != 3 {
		t.Fatal()
	}

	if queue.head.payload != 5 {
		t.Fatal()
	}

	if queue.Poll().(int) != 3 {
		t.Fatal()
	}

	if queue.Poll().(int) != 4 {
		t.Fatal()
	}

	if queue.Poll().(int) != 5 {
		t.Fatal()
	}

	if queue.Poll() != nil {
		t.Fatal()
	}
}

func TestQueue_Peek(t *testing.T) {
	queue := NewQueue(100)
	queue.Push(1)
	queue.Push(2)
	queue.Push(3)

	if queue.Peek().(int) != 1 {
		t.Fatal()
	}

	_ = queue.Poll()

	if queue.Peek().(int) != 2 {
		t.Fatal()
	}

	_ = queue.Poll()

	if queue.Peek().(int) != 3 {
		t.Fatal()
	}

	_ = queue.Poll()

	if queue.Peek() != nil {
		t.Fatal()
	}
}

func TestQueue_Poll(t *testing.T) {
	queue := NewQueue(5)

	if queue.Poll() != nil {
		t.Fatal()
	}

	for i := 0; i < 10; i++ {
		queue.Push(i)
	}

	if queue.Length() != 5 {
		t.Fatal()
	}

	if queue.head.payload.(int) != 9 {
		t.Fatal()
	}

	if queue.tail.payload.(int) != 5 {
		t.Fatal()
	}

	for i := 0; i < 5; i++ {
		_ = queue.Poll()
	}

	if queue.Length() != 0 {
		t.Fatal()
	}

	if queue.head != nil {
		t.Fatal()
	}

	if queue.tail != nil {
		t.Fatal()
	}
}

func TestQueue_largeSizeWithOverflow(t *testing.T) {
	queue := NewQueue(100)

	for i := 0; i < 1000; i++ {
		queue.Push(i)
	}

	for i := 0; i < 1000; i++ {
		_ = queue.Peek()
	}

	if queue.Length() != queue.maxLength {
		t.Fatal()
	}

	for i := 0; i < queue.Length(); i++ {
		_ = queue.Peek()
	}

	length := queue.Length()
	for i := 0; i < length-1; i++ {
		_ = queue.Poll()
	}

	poll := queue.Poll()
	if poll.(int) != 999 {
		t.Fatal()
	}

	if queue.Length() != 0 {
		t.Fatal()
	}

	if queue.head != nil {
		t.Fatal()
	}

	if queue.tail != nil {
		t.Fatal()
	}
}
