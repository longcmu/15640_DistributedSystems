package main

import (
	"net"
	"fmt"
)

// Quick test client to send messages to the problems, does not perform any extensive testing
func main() {
	fmt.Println("Begin")
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:12345")
	conn, _ := net.DialUDP("udp", nil, addr)
	bytes := make([]byte, 1)
	for i := 0; i < 100; i++ {
		bytes[0] = byte(i)
		fmt.Println(bytes)
		conn.Write(bytes)
	}
	fmt.Println("Done")
}
