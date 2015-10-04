// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"strconv"
	//"container/list"

	"github.com/cmu440/lspnet"
)

type cli struct {
	addr *lspnet.UDPAddr
}

type server struct {
	conn     lspnet.UDPConn
	curID    int
	seqNum   int
	clients  map[int]cli
	request  chan packet
	reply    chan packet
	dataRead chan packet
	count    chan int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	//println("Server NewServer")
	s := new(server)
	s.curID = 0
	s.clients = make(map[int]cli)
	s.seqNum = 0
	s.request = make(chan packet, 10000)
	s.reply = make(chan packet, 10000)
	s.dataRead = make(chan packet, 10000)
	s.count = make(chan int, 1)
	s.count <- 1

	addr, _ := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	// start listening
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		println("Listening error:", err.Error())
	}
	s.conn = *conn

	// put all incoming messages into request channel
	go func() {
		for {
			buf := make([]byte, 1024)
			size, clientAddr, err := conn.ReadFromUDP(buf)
			if err != nil {
				println("Some error %v", err)
				continue
			}
			m := Message{}
			json.Unmarshal(buf[0:size], &m)
			//println(m.String())
			s.request <- packet{&m, clientAddr, m.Type}
		}
	}()

	// handle request channel and put reply messages into reply channel
	go func() {
		for {
			p := <-s.request
			switch p.msgType {
			case MsgConnect:
				<-s.count
				s.clients[s.curID] = cli{p.addr}
				s.count <- 1
				s.reply <- packet{NewAck(s.curID, 0), p.addr, MsgAck}
				s.curID++
			case MsgData:
				s.dataRead <- p
				//s.reply <- packet{NewAck(p.msg.ConnID, p.msg.SeqNum), p.addr, MsgAck}
				s.reply <- packet{p.msg, p.addr, MsgData}
			case MsgAck:
				continue
			}
		}
	}()

	// handle reply channel
	go func() {
		for {
			p := <-s.reply
			msgB, _ := json.Marshal(*p.msg)
			_, err := conn.WriteToUDP(msgB, p.addr)
			if err != nil {
				println("the connection with the client has been lost")
			}
		}
	}()

	return s, nil
}

// Read reads a data message from a client and returns its payload,
// and the connection ID associated with the client that sent the message.
// This method should block until data has been received from some client.
// It should return a non-nil error if either (1) the connection to some
// client has been explicitly closed, (2) the connection to some client
// has been lost due to an epoch timeout and no other messages from that
// client are waiting to be returned, or (3) the server has been closed.
// In the first two cases, the client's connection ID and a non-nil
// error should be returned. In the third case, an ID with value 0 and
// a non-nil error should be returned.
func (s *server) Read() (int, []byte, error) {
	//println("Server Read")
	p := <-s.dataRead
	buf := make([]byte, 1024)
	size, _, err := s.conn.ReadFromUDP(buf)
	if err != nil {
		return p.msg.ConnID, nil, errors.New("the connection has been explicitly closed")
	}
	m := Message{}
	json.Unmarshal(buf[0:size], &m)

	return m.ConnID, m.Payload, nil
}

// Write sends a data message to the client with the specified connection ID.
// This method should NOT block, and should return a non-nil error if the
// connection with the client has been lost.
func (s *server) Write(connID int, payload []byte) error {
	//println("Server Write")
	c, ok := s.clients[connID]
	if !ok {
		return errors.New("the specified connection ID does not exist")
	}
	d := NewData(connID, s.seqNum, payload, nil)
	s.reply <- packet{d, c.addr, MsgData}
	s.seqNum++

	return nil
}

// CloseConn terminates the client with the specified connection ID, returning
// a non-nil error if the specified connection ID does not exist. All pending
// messages to the client should be sent and acknowledged. However, unlike Close,
// this method should NOT block.
func (s *server) CloseConn(connID int) error {
	//println("Server CloseConn")
	// c, ok := s.clients[connID]
	// if !ok {
	// 	return errors.New("the specified connection ID does not exist")
	// }
	// delete(s.clients, connID)

	return nil
}

// Close terminates all currently connected clients and shuts down the LSP server.
// This method should block until all pending messages for each client are sent
// and acknowledged. If one or more clients are lost during this time, a non-nil
// error should be returned. Once it returns, all goroutines running in the
// background should exit.
//
// You may assume that Read, Write, CloseConn, or Close will not be called after
// calling this method.
func (s *server) Close() error {
	//println("Server Close")
	// for k, _ := range s.clients {
	// 	c, ok := s.clients[k]
	// 	if !ok {
	// 		return errors.New("one or more clients are lost during this time")
	// 	}
	// 	delete(s.clients, k)
	// }

	return nil
}
