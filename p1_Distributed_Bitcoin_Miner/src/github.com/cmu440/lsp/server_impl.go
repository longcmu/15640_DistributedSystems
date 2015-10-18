// Andrew ID: longh
// Name: Long He
// go version: 1.42

// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/cmu440/lspnet"
)

type cli struct {
	addr       *lspnet.UDPAddr
	messaging  bool
	outSeqNum  int
	inSeqNum   int
	log        map[int]packet
	windowSize int
	epochCount int
	earlyBirds map[int]packet
	cliExit    chan int
}

type server struct {
	conn            lspnet.UDPConn
	curID           int
	clients         map[int]cli
	MsgConnect      chan packet
	MsgData         chan packet
	MsgAck          chan packet
	epochLimit      int
	epochMillis     int
	windowSize      int
	epochTrigger    chan int
	epochTickerExit chan int
	servingDesk     map[int]packet
	//readSignal chan packet
	readChannel         chan packet
	writeSignal         chan packet
	writeChannel        chan packet
	addrRegister        map[string]int
	closeConnSignal     chan int
	cliExitDone         chan int
	closeSignal         chan int
	cliConnClosed       chan int
	doormanExit         chan int
	eventHandlersExit   chan int
	closeDone           chan int
	readFromClosedSever chan int
}

// acknowledge a packet in the window and write a packet, if possible
func acknowledge(ack packet, c cli, s *server) {
	// a new ack permits a writing
	l, ok := c.log[ack.msg.SeqNum]
	if !ok {
		////println("acknowledge: not ok 1")
	}
	if l.acked == false {
		l.acked = true
		c.log[ack.msg.SeqNum] = l
		c.windowSize--
		// find a fitting packet to write
		for i := 1; i <= len(c.log); i++ {
			v, ok := c.log[i]
			if !ok {
				////println("acknowledge: not ok 2")
			}
			if v.wrote == false {
				v.msg.Hash = sha1Hash(v.msg.ConnID, v.msg.SeqNum, v.msg.Payload)
				msgB, err_m := json.Marshal(v.msg)
				if err_m != nil {
					////println(err_m.Error)
				}
				_, err_w := s.conn.WriteToUDP(msgB, ack.addr)
				if err_w != nil {
					////println(err_w.Error())
				}
				v.wrote = true
				c.log[i] = v
				c.windowSize++
				break
			}
		}
	}
	// update the client list
	s.clients[ack.msg.ConnID] = c
}

// put the packet into c.log and write a packet, if possible
func write(p packet, c cli, s *server) {
	// put the packet into c.log
	c.log[p.msg.SeqNum] = p
	// try to write a packet
	if c.windowSize < s.windowSize {
		// find the first fitting packet to write
		for i := 1; i <= len(c.log); i++ {
			v, ok := c.log[i]
			if !ok {
				////println("write: not ok")
			}
			if v.wrote == false {
				v.msg.Hash = sha1Hash(v.msg.ConnID, v.msg.SeqNum, v.msg.Payload)
				msgB, err_m := json.Marshal(v.msg)
				if err_m != nil {
					////println(err_m.Error())
				}
				_, err_w := s.conn.WriteToUDP(msgB, p.addr)
				if err_w != nil {
					////println(err_w.Error())
				}
				v.wrote = true
				c.log[i] = v
				c.windowSize++
				break
			}
		}
	}
	// update the client list
	s.clients[p.msg.ConnID] = c
}

// handle concurrent events
func EventHandlers(s *server) {
	for {
		select {
		case <-s.eventHandlersExit:
			return
		case <-s.epochTrigger:
			//////println(len(s.clients))
			totalWaitingCount := 0
			connLostDuringClose := 0
			for k, c := range s.clients {
				c.epochCount++
				//////println(c.epochCount)
				s.clients[k] = c
				////print("		")
				//////println(s.epochLimit)
				// if c.epochCount > s.epochLimit {
				// 	s.cliConnClosed <- k
				// 	connLostDuringClose = 1
				// }
				//////println(k)
				if c.messaging == false {
					// send an acknowledgment with sequence number 0
					msgB, err_m := json.Marshal(NewAck(k, 0))
					if err_m != nil {
						////println(err_m.Error())
					}
					_, err_w := s.conn.WriteToUDP(msgB, c.addr)
					if err_w != nil {
						////println(err_w.Error())
					}
				}
				totalWaitingCount = totalWaitingCount + c.windowSize
				// find fitting packets to write
				if c.windowSize != 0 {
					for i := 1; i <= len(c.log); i++ {
						v, ok := c.log[i]
						if !ok {
							////println("EventHandlers: not ok 1")
						}
						if v.wrote == true && v.acked == false {
							v.msg.Hash =
								sha1Hash(v.msg.ConnID, v.msg.SeqNum, v.msg.Payload)
							msgB, err_m := json.Marshal(v.msg)
							if err_m != nil {
								////println(err_m.Error())
							}
							_, err_w := s.conn.WriteToUDP(msgB, c.addr)
							if err_w != nil {
								////println(err_w.Error())
							}
						}
					}
				} else {
					waitingCount := 0
					for i := 1; i <= len(c.log); i++ {
						v, ok := c.log[i]
						if !ok {
							////println("cEventHandlers: not ok 2")
						}
						if v.wrote == false || v.acked == false {
							waitingCount++
						}
					}
					totalWaitingCount = totalWaitingCount + waitingCount
					if waitingCount == 0 {
						select {
						case <-c.cliExit:
							delete(s.clients, k)
							s.cliExitDone <- 1
						default:
							break
						}
					}
				}
			}
			if totalWaitingCount == 0 {
				select {
				case <-s.closeSignal:
					s.closeDone <- connLostDuringClose
				default:
					break
				}
			}
		case p := <-s.MsgConnect:
			// register a new client in client list
			//////println(p.msg.String())
			id, ok := s.addrRegister[p.addr.String()]
			if !ok {
				id = s.curID
				s.addrRegister[p.addr.String()] = id
				s.curID++
				s.clients[id] = cli{p.addr, false, 1, 1,
					make(map[int]packet), 0, 0, make(map[int]packet),
					make(chan int, 1)}
			}
			// send back connect ack
			msgB, err_m := json.Marshal(NewAck(id, 0))
			if err_m != nil {
				////println(err_m.Error())
			}
			//////println(s.curID)
			_, err_w := s.conn.WriteToUDP(msgB, p.addr)
			if err_w != nil {
				////println(err_w.Error)
			}
		case p := <-s.MsgData:
			//////println(p.msg.String())
			// make sure it's connected
			c, ok := s.clients[p.msg.ConnID]
			if !ok {
				////println("EventHandlers: not ok 3")
				s.MsgData <- p
				break
			}
			// a message was received from the other end in this epoch
			c.epochCount = 0
			// mark this client as messaging
			c.messaging = true
			// call read function and update inSeqNum of this client
			if p.msg.SeqNum == c.inSeqNum {
				_, ok = s.clients[p.msg.ConnID]
				if !ok {
					////println("EventHandlers: not ok 4")
					s.cliConnClosed <- p.msg.ConnID
					break
				}
				s.readChannel <- p
				c.inSeqNum++
				// don't read previous msg
			} else if p.msg.SeqNum > c.inSeqNum {
				c.earlyBirds[p.msg.SeqNum] = p
			}
			start := c.inSeqNum
			for i := start; i < 100; i++ {
				//////println(i)
				v, ok := c.earlyBirds[i]
				//////println(ok)
				//////println(c.inSeqNum)
				if ok && i == c.inSeqNum {
					s.readChannel <- v
					c.inSeqNum++
				}
			}
			// send back ack anyway
			msgB, err_m := json.Marshal(NewAck(p.msg.ConnID, p.msg.SeqNum))
			if err_m != nil {
				////println(err_m.Error())
			}
			_, err_w := s.conn.WriteToUDP(msgB, p.addr)
			if err_w != nil {
				////println(err_w.Error())
			}
			s.clients[p.msg.ConnID] = c
		case ack := <-s.MsgAck:
			// a message was received from the other end in this epoch
			c := s.clients[ack.msg.ConnID]
			c.epochCount = 0
			s.clients[ack.msg.ConnID] = c
			// mark corresponding msg in the window as acked
			c, ok := s.clients[ack.msg.ConnID]
			if !ok {
				////println("EventHandlers: not ok 5")
			}
			if ack.msg.SeqNum != 0 {
				acknowledge(ack, c, s)
			}
		//case p := <-s.readSignal:
		// send the msg to be read
		//s.readChannel <- p
		case p := <-s.writeSignal:
			// check if this client exist
			c, ok := s.clients[p.msg.ConnID]
			if !ok {
				////println("EventHandlers: not ok 6")
			}
			// add more info to this packet and update the client outSeqNum
			p.msg.SeqNum = c.outSeqNum
			p.addr = c.addr
			c.outSeqNum++
			s.clients[p.msg.ConnID] = c
			// try to write this packet
			write(p, c, s)
		case id := <-s.closeConnSignal:
			// check if this client exist
			//////println(id)
			c, ok := s.clients[id]
			if !ok {
				////println("EventHandlers: not ok 7")
			}
			c.cliExit <- 1
		}
	}
}

// put all incoming messages into their own channels
func doorman(s *server) {
	for {
		buf := make([]byte, 1024)
		size, clientAddr, err_r := s.conn.ReadFromUDP(buf)
		if err_r != nil {
			////print("					server: ")
			//////println(err_r.Error())
			select {
			case <-s.doormanExit:
				return
			default:
				continue
			}
			continue
		}
		m := Message{}
		err_u := json.Unmarshal(buf[0:size], &m)
		if err_u != nil {
			//print("doorman: ")
			////println(err_u.Error())
			continue
		}
		p := packet{&m, clientAddr, m.Type, false, false}
		//////println(p.msg.String())
		switch m.Type {
		case MsgConnect:
			////print("					server: ")
			//////println(p.msg.String())
			s.MsgConnect <- p
		case MsgData:
			if string(p.msg.Hash) !=
				string(sha1Hash(p.msg.ConnID, p.msg.SeqNum, p.msg.Payload)) {
				break
			}
			////print("					server: ")
			//////println(p.msg.String())
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
	//////println("Server NewServer")
	// initiate a server
	s := new(server)
	s.curID = 0
	s.clients = make(map[int]cli)
	s.MsgConnect = make(chan packet, 1000)
	s.MsgData = make(chan packet, 10000)
	s.MsgAck = make(chan packet, 10000)
	s.epochLimit = params.EpochLimit
	s.epochMillis = params.EpochMillis
	s.windowSize = params.WindowSize
	s.epochTrigger = make(chan int)
	s.epochTickerExit = make(chan int)
	s.servingDesk = make(map[int]packet)
	//s.readSignal = make(chan packet, 1000)
	s.readChannel = make(chan packet, 10000)
	s.writeSignal = make(chan packet, 1000)
	s.writeChannel = make(chan packet, 10000)
	s.addrRegister = make(map[string]int)
	s.closeConnSignal = make(chan int, 1)
	s.cliExitDone = make(chan int, 1)
	s.closeSignal = make(chan int)
	s.cliConnClosed = make(chan int, 100)
	s.doormanExit = make(chan int, 1)
	s.eventHandlersExit = make(chan int)
	s.closeDone = make(chan int)
	s.readFromClosedSever = make(chan int, 1)

	// start listening
	addr, err_r := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	if err_r != nil {
		////println(err_r.Error())
	}
	conn, err_l := lspnet.ListenUDP("udp", addr)
	if err_l != nil {
		////println(err_l.Error())
	}
	s.conn = *conn

	// start epochTicker
	go epochTicker(s.epochTrigger, s.epochTickerExit, s.epochMillis)
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
	//////println("Server Read")
	select {
	case p := <-s.readChannel:
		//////println(p.msg.String())
		return p.msg.ConnID, p.msg.Payload, nil
	case id := <-s.cliConnClosed:
		s.closeConnSignal <- id
		<-s.cliExitDone
		return id, nil, errors.New("client connection closed")
	case <-s.readFromClosedSever:
		return 0, nil, errors.New("the server has been closed")
	}
	// //print("					Server Read")
	// ////println(p.msg.String())
}

// Write sends a data message to the client with the specified connection ID.
// This method should NOT block, and should return a non-nil error if the
// connection with the client has been lost.
func (s *server) Write(connID int, payload []byte) error {
	//////println("Server Write")
	s.writeSignal <- packet{NewData(connID, 0, payload, nil), nil, MsgData, false, false}

	return nil
}

// CloseConn terminates the client with the specified connection ID, returning
// a non-nil error if the specified connection ID does not exist. All pending
// messages to the client should be sent and acknowledged. However,
// unlike Close, this method should NOT block.
func (s *server) CloseConn(connID int) error {
	//////println("Server CloseConn")
	s.closeConnSignal <- connID
	<-s.cliExitDone

	return nil
}

// Close terminates all currently connected clients and shuts down the LSP
// server. This method should block until all pending messages for each client
// are sent and acknowledged. If one or more clients are lost during this time,
// a non-nil error should be returned. Once it returns, all goroutines running
// in the background should exit.
//
//
// You may assume that Read, Write, CloseConn, or Close will not be called after
// calling this method.
func (s *server) Close() error {
	//////println("Server Close")
	s.closeSignal <- 1
	err := <-s.closeDone
	if err == 1 {
		return errors.New("one or more clients are lost during this time")
	}
	s.eventHandlersExit <- 1
	s.epochTickerExit <- 1
	s.doormanExit <- 1
	s.readFromClosedSever <- 1
	s.conn.Close()
	return nil
}
