package lsp

import (
	"github.com/cmu440/lspnet"
)

type packet struct {
	msg     *Message
	addr    *lspnet.UDPAddr
	msgType MsgType
}
