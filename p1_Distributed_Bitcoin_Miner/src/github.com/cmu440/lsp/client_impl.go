// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"

	"github.com/cmu440/lspnet"
)

type client struct {
	id     int
	seqNum int
	conn   lspnet.UDPConn
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
	c := new(client)
	c.seqNum = 0
	serverAddr, _ := lspnet.ResolveUDPAddr("udp", hostport)
	conn, err_d := lspnet.DialUDP("udp", nil, serverAddr)
	if err_d != nil {
		println("Dialing error:", err_d.Error())
	}
	c.conn = *conn

	msgD := NewConnect()
	msgB, _ := json.Marshal(msgD)
	_, err_w := c.conn.Write(msgB)
	if err_w != nil {
		println("NewConnect writing error:", err_w.Error())
	}

	buf := make([]byte, 1024)
	size, _, err_r := c.conn.ReadFromUDP(buf)
	if err_r != nil {
		println("NewConnect ack reading error:", err_r.Error())
	}
	m := Message{}
	json.Unmarshal(buf[0:size], &m)

	c.id = m.ConnID

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
	//println("Client Read")
	buf := make([]byte, 1024)
	size, _, err := c.conn.ReadFromUDP(buf)
	if err != nil {
		return nil, errors.New("the connection has been explicitly closed")
	}
	m := Message{}
	json.Unmarshal(buf[0:size], &m)
	//println(m.String())

	return m.Payload, nil
}

// Write sends a data message with the specified payload to the server.
// This method should NOT block, and should return a non-nil error
// if the connection with the server has been lost.
func (c *client) Write(payload []byte) error {
	//println("Client Write")
	d := NewData(c.id, c.seqNum, payload, nil)
	msgB, _ := json.Marshal(d)
	_, err := c.conn.Write(msgB)
	if err != nil {
		return errors.New("the connection with the server has been lost")
	}
	c.seqNum++

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
	c.conn.Close()
	return nil
}
