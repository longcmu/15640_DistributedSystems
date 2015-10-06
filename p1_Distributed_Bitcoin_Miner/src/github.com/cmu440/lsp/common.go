// Andrew ID: longh
// Name: Long He
// go version: 1.42

package lsp

import (
	"time"

	"github.com/cmu440/lspnet"
)

type packet struct {
	msg     *Message
	addr    *lspnet.UDPAddr
	msgType MsgType
}

func epochTicker(epochTrigger chan int, epochExit chan int, epochMillis int) {
	for {
		select {
		case <-epochExit:
			return
		case <-time.After(time.Millisecond * time.Duration(epochMillis)):
			epochTrigger <- 1
		}
	}
}
