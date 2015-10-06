// Andrew ID: longh
// Name: Long He
// go version: 1.42

// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	//"errors"
	"strconv"

	"github.com/cmu440/lspnet"
)

type cli struct {
	addr *lspnet.UDPAddr
	messaging bool
	seqNum int
}

type server struct {
	conn lspnet.UDPConn
	curID int
	seqNum int
	clients map[int]cli
	MsgConnect chan packet
	MsgData chan packet
	MsgAck chan packet
	epochLimit int
	epochMillis int
	windowSize int
	epochTrigger chan int
	epochExit chan int
	servingDesk map[int]packet
	readSignal chan packet
	readChannel chan packet
	readPool chan packet
	writeSignal chan packet
	writeChannel chan packet
}

// handle concurrent events
func EventHandlers(s *server) {
	for {
		select {
		// close to be done
		case <-s.epochTrigger:
			for k, cli := range s.clients {
				if cli.messaging == false {
					// send an acknowledgment with sequence number 0
					msgB, _ := json.Marshal(packet{NewAck(k, 0), cli.addr, MsgAck})
					_, err := s.conn.WriteToUDP(msgB, cli.addr)
					if err != nil {
						println("the connection with the client has been lost")
					}
				}
			}
			if len(s.servingDesk) != 0 {
				println(len(s.servingDesk))
				for k, p := range s.servingDesk {
					//print("server epoch:")
					//println(p.msg.String())
					msgB, _ := json.Marshal(packet{p.msg, nil, MsgData})
					_, err := s.conn.WriteToUDP(msgB, p.addr)
					if err != nil {
						println("the connection with the client has been lost")
					}
					delete(s.servingDesk, k)
				}
			}
		case p := <-s.MsgConnect:
			s.clients[s.curID] = cli{p.addr, false, -1}
			msgB, _ := json.Marshal(NewAck(s.curID, 0))
			_, err := s.conn.WriteToUDP(msgB, p.addr)
			if err != nil {
				println("the connection with the client has been lost")
			}
			s.curID++
		case p := <-s.MsgData:
			s.clients[p.msg.ConnID] = cli{p.addr, true, p.msg.SeqNum}
			s.readSignal <- p
			msgB, _ := json.Marshal(NewAck(p.msg.ConnID, p.msg.SeqNum))
			_, err := s.conn.WriteToUDP(msgB, p.addr)
			if err != nil {
				println("the connection with the client has been lost")
			}
		case  ack := <-s.MsgAck:
			_, ok := s.servingDesk[ack.msg.ConnID] 
			if ok {
				delete(s.servingDesk, ack.msg.ConnID)
			}
			for len(s.servingDesk) < s.windowSize && len(s.writeChannel) != 0 {
				p := <- s.writeChannel
				c, ok := s.clients[p.msg.ConnID]
				if !ok {
					println("the connection with the client has been lost")
				}
				msgB, _ := json.Marshal(p.msg)
				_, err := s.conn.WriteToUDP(msgB, c.addr)
				if err != nil {
					println("the connection with the client has been lost")
				}
				s.servingDesk[p.msg.ConnID] = p
			}
		case p := <-s.readSignal:
			s.readChannel <- p
		case p := <-s.writeSignal:
			s.writeChannel <- p
			if s.seqNum == 0 {
				s.MsgAck <- packet{NewAck(-1, -1), nil, MsgAck}
			}
		}
	}
}

// put all incoming messages into their own channels
func doorman(s *server) {
	for {
		buf := make([]byte, 1024)
		size, clientAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			println("Some error %v", err)
			continue
		}
		m := Message{}
		json.Unmarshal(buf[0:size], &m)
		p := packet{&m, clientAddr, m.Type}
		switch m.Type {
		case MsgConnect:
			s.MsgConnect <- p
		case MsgData:
			s.MsgData <- p
		case MsgAck:
			s.MsgAck <- p
		}
	}
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	//println("Server NewServer")
	// initiate a server
	s := new(server)
	s.curID = 0
	s.seqNum = 0
	s.clients = make(map[int]cli)
	s.MsgConnect = make(chan packet, 100)
	s.MsgData = make(chan packet, 1000)
	s.MsgAck = make(chan packet, 1000)
	s.epochLimit = params.EpochLimit
	s.epochMillis = params.EpochMillis
	s.windowSize = params.WindowSize
	s.epochTrigger = make(chan int)
	s.epochExit = make(chan int)
	s.servingDesk = make(map[int]packet)
	s.readSignal = make(chan packet, 100)
	s.readChannel = make(chan packet, 1000)
	s.readPool = make(chan packet, 1000)
	s.writeSignal = make(chan packet, 100)
	s.writeChannel = make(chan packet, 1000)

	// start listening
	addr, _ := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		println("Listening error:", err.Error())
	}
	s.conn = *conn

	// start epochTicker
	go epochTicker(s.epochTrigger, s.epochExit, s.epochMillis)
	// handle concurrent events
	go EventHandlers(s)
	// handle incoming packets
	go doorman(s)

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
	p := <-s.readChannel

	return p.msg.ConnID, p.msg.Payload, nil
}

// Write sends a data message to the client with the specified connection ID.
// This method should NOT block, and should return a non-nil error if the
// connection with the client has been lost.
func (s *server) Write(connID int, payload []byte) error {
	//println("Server Write")
	s.writeSignal <- packet{NewData(connID, s.seqNum, payload, nil), nil, MsgData}

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
