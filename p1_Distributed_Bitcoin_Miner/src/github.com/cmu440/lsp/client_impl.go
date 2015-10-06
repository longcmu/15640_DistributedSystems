// Andrew ID: longh
// Name: Long He
// go version: 1.42

// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	//"errors"

	"github.com/cmu440/lspnet"
)

type client struct {
	conn   lspnet.UDPConn
	id     int
	seqNum int
	connected bool
	messaged bool
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
func cEventHandlers(c *client) {
	for {
		select {
		// close to be done
		case <-c.epochTrigger:
			if c.connected == false {
				// resent NewConnect
				msgD := NewConnect()
				msgB, _ := json.Marshal(msgD)
				_, err_w := c.conn.Write(msgB)
				if err_w != nil {
					println("NewConnect writing error:", err_w.Error())
				}
			}
			if c.messaged == false {
				// send an acknowledgment with sequence number 0
				msgB, _ := json.Marshal(packet{NewAck(c.id, 0), nil, MsgAck})
				_, err := c.conn.Write(msgB)
				if err != nil {
					println("the connection with the server has been lost")
				}
			}
			if len(c.servingDesk) != 0 {
				for k, p := range c.servingDesk {
					//print("client epoch: ")
					//println(p.msg.String())
					msgB, _ := json.Marshal(packet{p.msg, nil, MsgData})
					_, err := c.conn.Write(msgB)
					if err != nil {
						println("the connection with the server has been lost")
					}
					delete(c.servingDesk, k)
				}
			}
		case p := <- c.MsgData:
			c.readSignal <- p
			msgB, _ := json.Marshal(NewAck(c.id, p.msg.SeqNum))
			_, err := c.conn.Write(msgB)
			if err != nil {
				println("the connection with the server has been lost")
			}
		case ack := <-c.MsgAck:
			_, ok := c.servingDesk[ack.msg.ConnID]
			if ok && ack.msg.ConnID != -1 {
				delete(c.servingDesk, ack.msg.ConnID)
			}
			for len(c.servingDesk) < c.windowSize && len(c.writeChannel) != 0 {
				p := <- c.writeChannel
				msgB, _ := json.Marshal(p.msg)
				_, err := c.conn.Write(msgB)
				if err != nil {
					println("the connection with the server has been lost")
				}
				c.servingDesk[p.msg.ConnID] = p
			}
		case p := <-c.readSignal:
			c.readChannel <- p
		case p := <-c.writeSignal:
			c.writeChannel <- p
			if c.seqNum == 0 {
				c.MsgAck <- packet{NewAck(-1, -1), nil, MsgAck}
			}
		}
	}
}

// put all incoming messages into incomingPackets channel
func cdoorman(c *client) {
	for {
		buf := make([]byte, 1024)
		size, err := c.conn.Read(buf)
		if err != nil {
			println("Some error %v", err)
			continue
		}
		m := Message{}
		json.Unmarshal(buf[0:size], &m)
		p := packet{&m, nil, m.Type}
		switch m.Type {
		case MsgData:
			c.MsgData <- p
		case MsgAck:
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
	c.seqNum = 0
	c.connected = false
	c.messaged = false
	c.MsgData  = make(chan packet, 1000)
	c.MsgAck  = make(chan packet, 1000)
	c.epochLimit = params.EpochLimit
	c.epochMillis = params.EpochMillis
	c.windowSize = params.WindowSize
	c.epochTrigger = make(chan int)
	c.epochExit = make(chan int)
	c.servingDesk = make(map[int]packet)
	c.readSignal = make(chan packet, 100)
	c.readChannel = make(chan packet, 1000)
	c.readPool = make(chan packet, 1000)
	c.writeSignal = make(chan packet, 100)
	c.writeChannel = make(chan packet, 1000)

	// start epochTicker
	go epochTicker(c.epochTrigger, c.epochExit, c.epochMillis)

	// dial udp
	serverAddr, _ := lspnet.ResolveUDPAddr("udp", hostport)
	conn, err_d := lspnet.DialUDP("udp", nil, serverAddr)
	if err_d != nil {
		println("Dialing error:", err_d.Error())
	}
	c.conn = *conn

	// sent NewConnect
	msgD := NewConnect()
	msgB, _ := json.Marshal(msgD)
	_, err_w := c.conn.Write(msgB)
	if err_w != nil {
		println("NewConnect writing error:", err_w.Error())
	}

	// receive NewConnect ack
	buf := make([]byte, 1024)
	size, _, err_r := c.conn.ReadFromUDP(buf)
	if err_r != nil {
		println("NewConnect ack reading error:", err_r.Error())
	}
	m := Message{}
	json.Unmarshal(buf[0:size], &m)
	c.connected = true
	c.id = m.ConnID

	// handle concurrent events
	go cEventHandlers(c)
	// handle incoming packets
	go cdoorman(c)

	return c, nil
}

// ConnID returns the connection ID associated with this client.
func (c *client) ConnID() int {
	return c.id
}

// Read reads a data message from the server and returns its payload.
// This method should block until data has been received from the server and
// is ready to be returned. It should return a non-nil error if either
// (1) the connection has been explicitly closed, or (2) the connection has
// been lost due to an epoch timeout and no other messages are waiting to be
// returned.
func (c *client) Read() ([]byte, error) {
	p := <-c.readChannel

	return p.msg.Payload, nil
}

// Write sends a data message with the specified payload to the server.
// This method should NOT block, and should return a non-nil error
// if the connection with the server has been lost.
func (c *client) Write(payload []byte) error {
	//println("Client Write")
	c.writeSignal <- packet{NewData(c.id, c.seqNum, payload, nil), nil, MsgData}

	return nil
}

// Close terminates the client's connection with the server. It should block
// until all pending messages to the server have been sent and acknowledged.
// Once it returns, all goroutines running in the background should exit.
//
// You may assume that Read, Write, and Close will not be called after
// Close has been called.
func (c *client) Close() error {
	//println("Client Close")
	//c.conn.Close()
	return nil
}
