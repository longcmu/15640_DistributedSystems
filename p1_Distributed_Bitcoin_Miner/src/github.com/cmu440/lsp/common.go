// Andrew ID: longh
// Name: Long He
// go version: 1.42

package lsp

import (
	"crypto/sha1"
	"strconv"
	"time"

	"github.com/cmu440/lspnet"
)

type packet struct {
	msg     *Message
	addr    *lspnet.UDPAddr
	msgType MsgType
	acked   bool
	wrote   bool
}

func sha1Hash(connID, seqNum int, payload []byte) []byte {
	h := sha1.New()
	h.Write([]byte(strconv.Itoa(connID)))
	h.Write([]byte(strconv.Itoa(seqNum)))
	h.Write(payload)
	return h.Sum(nil)
	return nil
}

func epochTicker(epochTrigger chan int, epochExit chan int, epochMillis int) {
	for {
		select {
		case <-epochExit:
			return
		case <-time.After(time.Millisecond * time.Duration(epochMillis)):
			//epochTrigger <- 1
		}
	}
}
