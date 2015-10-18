// Andrew ID: longh
// Name: Long He
// go version: 1.42

// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"

	"github.com/cmu440/lspnet"
)

type client struct {
	conn            lspnet.UDPConn
	id              int
	outSeqNum       int
	inSeqNum        int
	connected       bool
	messaged        bool
	MsgData         chan packet
	MsgAck          chan packet
	epochLimit      int
	epochMillis     int
	windowSize      int
	epochTrigger    chan int
	epochTickerExit chan int
	servingDesk     map[int]packet
	//readSignal   chan packet
	readChannel           chan packet
	writeSignal           chan packet
	writeChannel          chan packet
	log                   map[int]packet
	winSize               int
	epochCount            int
	earlyBirds            map[int]packet
	cdoormanExit          chan int
	writeBeforeConnected  chan packet
	connClosed            chan int
	cEventHandlersExit    chan int
	clearPending          chan int
	connLost              chan int
	writeToLostConnection chan int
}

// acknowledge a packet in the window and write a packet, if possible
func cacknowledge(ack packet, c *client) {
	// a new ack permits a writing
	l, ok := c.log[ack.msg.SeqNum]
	if !ok {
		//println("cacknowledge: not ok")
	}
	if l.acked == false {
		l.acked = true
		c.log[ack.msg.SeqNum] = l
		c.winSize--
		// find a fitting packet to write
		for i := 1; i <= len(c.log); i++ {
			v := c.log[i]
			if v.wrote == false {
				v.msg.Hash = sha1Hash(v.msg.ConnID, v.msg.SeqNum, v.msg.Payload)
				msgB, err_m := json.Marshal(v.msg)
				if err_m != nil {
					//println(err_m.Error())
				}
				_, err_w := c.conn.Write(msgB)
				if err_w != nil {
					//println(err_w.Error())
				}
				v.wrote = true
				c.log[i] = v
				c.winSize++
				break
			}
		}
	}
}

// put the packet into c.log and write a packet, if possible
func cwrite(p packet, c *client) {
	// put the packet into c.log
	c.log[p.msg.SeqNum] = p
	// try to write a packet
	if c.winSize < c.windowSize {
		// find the first fitting packet to write
		for i := 1; i <= len(c.log); i++ {
			v, ok := c.log[i]
			if !ok {
				//println("cwrite: not ok")
			}
			if v.wrote == false {
				v.msg.Hash = sha1Hash(v.msg.ConnID, v.msg.SeqNum, v.msg.Payload)
				msgB, err_m := json.Marshal(v.msg)
				if err_m != nil {
					//println(err_m.Error())
				}
				_, err_w := c.conn.Write(msgB)
				if err_w != nil {
					//println(err_w.Error())
				}
				v.wrote = true
				c.log[i] = v
				c.winSize++
				break
			}
		}
	}
}

// handle concurrent events
func cEventHandlers(c *client) {
	for {
		select {
		case <-c.cEventHandlersExit:
			return
		case <-c.epochTrigger:
			c.epochCount++
			// println(c.epochCount)
			// print("    ")
			// println(c.epochLimit)
			if c.epochCount > c.epochLimit {
				c.connLost <- 1
				c.writeToLostConnection <- 1
			}
			if c.connected == false {
				c.epochCount = 0
				// resent NewConnect
				msgD := NewConnect()
				msgB, err_m := json.Marshal(msgD)
				if err_m != nil {
					//println(err_m.Error())
				}
				_, err_w := c.conn.Write(msgB)
				if err_w != nil {
					//println(err_w.Error())
				}
				break
			}
			if c.messaged == false {
				//println("4")
				// send an acknowledgment with sequence number 0
				msgB, err_m := json.Marshal(NewAck(c.id, 0))
				if err_m != nil {
					//println(err_m.Error())
				}
				_, err_w := c.conn.Write(msgB)
				if err_w != nil {
					//println(err_w.Error())
				}
			}
			// find fitting packets to write
			if c.winSize != 0 {
				//println("1")
				// for _, v := range c.log {
				// 	print("& ")
				// 	println(v.msg.String())
				// }
				for i := 1; i <= len(c.log); i++ {
					v, ok := c.log[i]
					if !ok {
						//println("cEventHandlers: not ok 1")
					}
					if v.wrote == true && v.acked == false {
						v.msg.Hash =
							sha1Hash(v.msg.ConnID, v.msg.SeqNum, v.msg.Payload)
						msgB, err_m := json.Marshal(v.msg)
						if err_m != nil {
							//println(err_m.Error())
						}
						_, err_w := c.conn.Write(msgB)
						if err_w != nil {
							//println(err_w.Error())
						}
					}
				}
			} else {
				waitingCount := 0
				for i := 1; i <= len(c.log); i++ {
					v, ok := c.log[i]
					if !ok {
						//println("cEventHandlers: not ok 2")
					}
					if v.wrote == false || v.acked == false {
						waitingCount++
					}
				}
				if waitingCount == 0 {
					select {
					case <-c.clearPending:
					default:
						break
					}
				}
			}
		case p := <-c.MsgData:
			//print("receive2: ")
			//println(p.msg.String())
			// a message was received from the other end in this epoch
			c.epochCount = 0
			// mark client as messaged
			c.messaged = true
			// call read function and update inSeqNum of this client
			if p.msg.SeqNum == c.inSeqNum {
				c.readChannel <- p
				c.inSeqNum++
				// don't read previous msg
			} else if p.msg.SeqNum > c.inSeqNum {
				c.earlyBirds[p.msg.SeqNum] = p
			}
			start := c.inSeqNum
			if len(c.earlyBirds) != 0 {
				for i := start; i < 100; i++ {
					v, ok := c.earlyBirds[i]
					if !ok {
						//println("cEventHandlers: not ok 3")
					}
					if ok && i == c.inSeqNum {
						c.readChannel <- v
						c.inSeqNum++
					}
				}
			}
			// send back ack anyway
			msgB, err_m := json.Marshal(NewAck(p.msg.ConnID, p.msg.SeqNum))
			if err_m != nil {
				//println(err_m.Error())
			}
			_, err_w := c.conn.Write(msgB)
			if err_w != nil {
				//println(err_w.Error())
			}
		case ack := <-c.MsgAck:
			// a message was received from the other end in this epoch
			c.epochCount = 0
			// mark corresponding msg in the window as acked
			if ack.msg.SeqNum != 0 {
				cacknowledge(ack, c)
			} else if c.connected == false {
				c.connected = true
				c.id = ack.msg.ConnID
				c.writeSignal <- packet{NewData(-1, 0, nil, nil), nil, MsgData, false, false}
			}
		//case p := <-c.readSignal:
		// send the msg to be read
		//c.readChannel <- p
		case p := <-c.writeSignal:
			if p.msg.ConnID != -1 {
				//println(p.msg.String())
				// add more info to this packet and update outSeqNum
				p.msg.SeqNum = c.outSeqNum
				// print("write: ")
				// println(p.msg.String())
				c.outSeqNum++
			}
			// try to write this packet
			if c.connected == true {
				for len(c.writeBeforeConnected) != 0 {
					out := <-c.writeBeforeConnected
					out.msg.ConnID = c.id
					cwrite(out, c)
				}
				if p.msg.ConnID != -1 {
					cwrite(p, c)
				}
			} else if p.msg.ConnID != -1 {
				c.writeBeforeConnected <- p
			}
		}
	}
}

// put all incoming messages into their own channel
func cdoorman(c *client) {
	for {
		select {
		case <-c.cdoormanExit:
			return
		default:
			break
		}
		buf := make([]byte, 1024)
		size, err_r := c.conn.Read(buf)
		if err_r != nil {
			str := err_r.Error()
			str = str[len(str)-32 : len(str)]
			if str == "use of closed network connection" {
				c.connClosed <- 1
				return
			}
			//print("cdoorman: ")
			//println(err_r.Error())
		}
		m := Message{}
		err_u := json.Unmarshal(buf[0:size], &m)
		if err_u != nil {
			//print("cdoorman: ")
			//println(err_u.Error())
			continue
		}
		p := packet{&m, nil, m.Type, false, false}
		switch m.Type {
		case MsgData:
			if string(p.msg.Hash) !=
				string(sha1Hash(p.msg.ConnID, p.msg.SeqNum, p.msg.Payload)) {
				continue
			}
			//print("receive1: ")
			//println(p.msg.String())
			c.MsgData <- p
		case MsgAck:
			//print("receive: ")
			//println(p.msg.String())
			c.MsgAck <- p
		}
	}
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	//println("Client NewClient")
	// initiate a client
	c := new(client)
	c.outSeqNum = 1
	c.inSeqNum = 1
	c.connected = false
	c.messaged = false
	c.MsgData = make(chan packet, 10000)
	c.MsgAck = make(chan packet, 10000)
	c.epochLimit = params.EpochLimit
	c.epochMillis = params.EpochMillis
	c.windowSize = params.WindowSize
	c.epochTrigger = make(chan int)
	c.epochTickerExit = make(chan int)
	c.servingDesk = make(map[int]packet)
	//c.readSignal = make(chan packet, 1000)
	c.readChannel = make(chan packet, 10000)
	c.writeSignal = make(chan packet, 1000)
	c.writeChannel = make(chan packet, 10000)
	c.log = make(map[int]packet)
	c.epochCount = 0
	c.cEventHandlersExit = make(chan int)
	c.earlyBirds = make(map[int]packet)
	c.cdoormanExit = make(chan int, 1)
	c.writeBeforeConnected = make(chan packet, 100)
	c.connClosed = make(chan int, 1)
	c.connLost = make(chan int, 10)
	c.clearPending = make(chan int)
	c.writeToLostConnection = make(chan int, 1)
	c.writeToLostConnection <- 0

	// start epochTicker
	go epochTicker(c.epochTrigger, c.epochTickerExit, c.epochMillis)

	// dial udp
	serverAddr, err_r := lspnet.ResolveUDPAddr("udp", hostport)
	if err_r != nil {
		//println(err_r.Error())
	}
	conn, err_d := lspnet.DialUDP("udp", nil, serverAddr)
	if err_d != nil {
		//println(err_d.Error())
	}
	c.conn = *conn

	// sent NewConnect
	msgD := NewConnect()
	msgB, err_m := json.Marshal(msgD)
	if err_m != nil {
		//println(err_m.Error())
	}
	_, err_w := c.conn.Write(msgB)
	if err_w != nil {
		//println(err_w.Error())
	}

	// receive NewConnect ack
	buf := make([]byte, 1024)
	size, _, err_r := c.conn.ReadFromUDP(buf)
	if err_r != nil {
		//print("NewClient: receive NewConnect ack ")
		//println(err_r.Error())
	} else {
		c.connected = true
		//println(c.epochCount)
		m := Message{}
		err_u := json.Unmarshal(buf[0:size], &m)
		if err_u != nil {
			//print("NewClient: ")
			//println(err_u.Error())
		}
		c.id = m.ConnID
	}

	// handle concurrent events
	go cEventHandlers(c)
	// handle incoming packets
	go cdoorman(c)

	return c, nil
}

// ConnID returns the connection ID associated with this client.
func (c *client) ConnID() int {
	//println("Client ConnID")
	return c.id
}

// Read reads a data message from the server and returns its payload.
// This method should block until data has been received from the server and
// is ready to be returned. It should return a non-nil error if either
// (1) the connection has been explicitly closed, or (2) the connection has
// been lost due to an epoch timeout and no other messages are waiting to be
// returned.
func (c *client) Read() ([]byte, error) {
	//println("Client Read")
	select {
	case p := <-c.readChannel:
		return p.msg.Payload, nil
	case <-c.connClosed:
		return nil, errors.New("the connection has been explicitly closed")
	case <-c.connLost:
		// the connection lost due to an epoch timeout and no other messages
		// are waiting to be returned
		return nil, errors.New("connection lost")
	}
	//println(p.msg.String())
}

// Write sends a data message with the specified payload to the server.
// This method should NOT block, and should return a non-nil error
// if the connection with the server has been lost.
func (c *client) Write(payload []byte) error {
	//println("Client Write")
	c.writeSignal <- packet{NewData(c.id, 0, payload, nil), nil, MsgData, false, false}
	i := <-c.writeToLostConnection
	c.writeToLostConnection <- i
	if i == 1 {
		return errors.New("the connection with the server has been lost")
	} else {
		return nil
	}
}

// Close terminates the client's connection with the server. It should block
// until all pending messages to the server have been sent and acknowledged.
// Once it returns, all goroutines running in the background should exit.
//
// You may assume that Read, Write, and Close will not be called after
// Close has been called.
func (c *client) Close() error {
	//println("Client Close")
	c.clearPending <- 1
	//println("done")
	c.cEventHandlersExit <- 1
	c.epochTickerExit <- 1
	c.cdoormanExit <- 1
	//println("Client Close Done")
	return nil
}
