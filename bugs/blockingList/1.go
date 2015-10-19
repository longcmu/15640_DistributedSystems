package main

import (
	"container/list"
	"sync"
	"fmt"
)

type BlockingList struct {
	list *list.List
	num int
	mutex sync.Mutex
}

func NewBlockingList() (*BlockingList) {
	bl := new(BlockingList)
	bl.list = list.New()
	return bl
}

func (bl *BlockingList) Insert(val interface{}) {
	bl.mutex.Lock()
	bl.list.PushBack(val)
	bl.num++
	bl.mutex.Unlock()
}

func (bl *BlockingList) Remove() interface{} {
	var val interface{}
	done := false
	for !done {
		bl.mutex.Lock()
		if bl.num > 0 {
			val = bl.list.Remove(bl.list.Front())
			bl.num--
			done = true
		}
		bl.mutex.Unlock()
	}
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
