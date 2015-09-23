package main

import (
	"net"
	"bufio"
	"strconv"
)

const (
	defaultHost = "localhost"
	defaultPort = 9999
)

// To test your server implementation, you might find it helpful to implement a
// simple 'client runner' program. The program could be very simple, as long as
// it is able to connect with and send messages to your server and is able to
// read and print out the server's echoed response to standard output. Whether or
// not you add any code to this file will not affect your grade.
func main() {
	conn, _ := net.Dial("tcp", ":" + strconv.Itoa(defaultPort))

	for i := 0; i < 10000; i++ {
		message := strconv.Itoa(i) + "\n"
		conn.Write([]byte(message))
		msg, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			println("Reading error:", err.Error())
		}
		print(string(msg))
	}

	select{}
}
