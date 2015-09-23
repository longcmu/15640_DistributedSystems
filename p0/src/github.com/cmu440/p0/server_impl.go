// Andrew ID: longh
// Name: Long He
// go version: 1.42

package p0

import (
	"bufio"
	"net"
	"strconv"
)

type client struct {
	messageReadChannel  chan string
	messageWriteChannel chan string
	connection          net.Conn
}

type multiEchoServer struct {
	listener       net.Listener
	messageChannel chan string
	clients        []client
	clientsNumber  chan int
}

// Add new clients into multiEchoServer
func (mes *multiEchoServer) AddClient(conn net.Conn) {
	c := client{make(chan string, 1), make(chan string, 75), conn}
	count := <-mes.clientsNumber
	mes.clients = append(mes.clients, c)
	mes.clientsNumber <- count + 1
	reader := bufio.NewReader(conn)
	// keep reading messages from connection and put them into
	// messageReadChannel of clients
	go func() {
		for {
			message, err := reader.ReadString('\n')
			if err != nil {
				count := <-mes.clientsNumber
				conn.Close()
				mes.clientsNumber <- count - 1
				//println("Reading error:", err.Error())
				return
			}
			c.messageReadChannel <- message
		}
	}()

	// keep writting messages from messageWriteChannel of clients
	go func() {
		for message := range c.messageWriteChannel {
			doubledMessage := message[:len(message)-1] + message
			_, err := conn.Write([]byte(doubledMessage))
			if err != nil {
				//println("Writing error:", err.Error())
			}
		}
	}()

	// keep sending messages from messageReadChannel of clients into the
	// messageChannel of server
	go func() {
		for {
			mes.messageChannel <- <-c.messageReadChannel
		}
	}()
}

// Broadcast takes messages and sends them to messageWriteChannel of each
// client
func (mes *multiEchoServer) Broadcast(message string) {
	count := <-mes.clientsNumber
	for _, client := range mes.clients {
		if len(client.messageWriteChannel) < 75 {
			client.messageWriteChannel <- message
		} else {
			continue
		}
	}
	mes.clientsNumber <- count
}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	server := new(multiEchoServer)
	server.listener = nil
	server.messageChannel = make(chan string, 1)
	server.clients = nil
	server.clientsNumber = make(chan int, 1)
	server.clientsNumber <- 0
	// keep broadcast messages from messageChannel of server to each
	// client
	go func() {
		for {
			select {
			case message := <-server.messageChannel:
				server.Broadcast(message)
			}
		}
	}()
	return server
}

// Start gets port number and starts a listener to build connections
func (mes *multiEchoServer) Start(port int) error {
	// start listening
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		//println("Listening error:", err.Error())
		return err
	}
	mes.listener = listener

	// keep accepting incoming connections and add clients into
	// multiEchoServer
	go func() {
		for {
			connection, err := mes.listener.Accept()
			if err != nil {
				//println("Accepting error:", err.Error())
				return
			}
			mes.AddClient(connection)
		}
	}()
	return nil
}

// close all connections and the server
func (mes *multiEchoServer) Close() {
	for _, client := range mes.clients {
		client.connection.Close()
	}
	mes.listener.Close()
}

// Count returns the number of current active clients
func (mes *multiEchoServer) Count() int {
	count := <-mes.clientsNumber
	mes.clientsNumber <- count
	return count
}
