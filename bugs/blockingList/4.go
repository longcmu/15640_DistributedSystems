package main

import (
	"container/list"
	"fmt"
)

type BlockingList struct {
	list *list.List
	num int
	insertRequestCh chan interface{}
	insertReplyCh chan bool
	removeRequestCh chan int
	removeReplyCh chan interface{}
}

func NewBlockingList() (*BlockingList) {
	bl := new(BlockingList)
	bl.list = list.New()
	bl.insertRequestCh = make(chan interface{})
	bl.insertReplyCh = make(chan bool)
	bl.removeRequestCh = make(chan int)
	bl.removeReplyCh = make(chan interface{})
	go bl.Handler()
	return bl
}

func (bl *BlockingList) Handler() {
	for {
		if bl.num > 0 {
			select {
			case val := <- bl.insertRequestCh:
				bl.list.PushBack(val)
				bl.num++
				bl.insertReplyCh <- true
			case <- bl.removeRequestCh:
				bl.removeReplyCh <- bl.list.Remove(bl.list.Front())
				bl.num--
			}
		} else {
			select {
			case val := <- bl.insertRequestCh:
				bl.list.PushBack(val)
				bl.num++
				bl.insertReplyCh <- true
			}
		}
	}
}

func (bl *BlockingList) Insert(val interface{}) {
	bl.insertRequestCh <- val
	<- bl.insertReplyCh
}

func (bl *BlockingList) Remove() interface{} {
	bl.removeRequestCh <- 1
	return <- bl.removeReplyCh
}

func main() {
	fmt.Println("Begin")
	bl1 := NewBlockingList()
	bl2 := NewBlockingList()
	go RemoveInsert(bl1, bl2)
	InsertRemove(bl1, bl2)
	fmt.Println("Done")
}

func RemoveInsert(bl1, bl2 *BlockingList) {
	bl2.Insert(bl1.Remove())
}

func InsertRemove(bl1, bl2 *BlockingList) {
	bl1.Insert(0)
	bl2.Remove()
}
