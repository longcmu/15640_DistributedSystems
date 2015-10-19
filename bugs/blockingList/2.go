package main

import (
	"container/list"
	"fmt"
)

type BlockingList struct {
	list *list.List
	num int
	insertCh chan interface{}
	removeCh chan interface{}
}

func NewBlockingList() (*BlockingList) {
	bl := new(BlockingList)
	bl.list = list.New()
	bl.insertCh = make(chan interface{})
	bl.removeCh = make(chan interface{})
	go bl.Handler()
	return bl
}

func (bl *BlockingList) Handler() {
	for {
		select {
		case val := <- bl.insertCh:
			bl.list.PushBack(val)
			bl.num++
		default:
			if bl.num > 0 {
				select {
				case bl.removeCh <- bl.list.Front().Value:
					bl.list.Remove(bl.list.Front())
					bl.num--
				default:
				}
			}
		}
	}
}

func (bl *BlockingList) Insert(val interface{}) {
	bl.insertCh <- val
}

func (bl *BlockingList) Remove() interface{} {
	val := <- bl.removeCh
	return val
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
