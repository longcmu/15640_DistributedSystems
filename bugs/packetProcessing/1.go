package main

import (
	"net"
	"runtime"
	"fmt"
)

type UDPDaemon struct {
	conn *net.UDPConn
	packetCh chan []byte
	doneCh chan bool
}

func NewUDPDaemon(port int) (*UDPDaemon, error) {
	d := &UDPDaemon{}
	d.packetCh = make(chan []byte)
	d.doneCh = make(chan bool)
	// Create connection
	addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	d.conn, _ = net.ListenUDP("udp", addr)
	// Start read handler
	go d.ReadHandler()
	// Start a process handler for each CPU core
	for i := 0; i < runtime.NumCPU(); i++ {
		go d.ProcessHandler()
	}
	return d, nil
}

func (d *UDPDaemon) ReadHandler() {
	bytes := make([]byte, 2000)
	for {
		// Read packet
		n, _, _ := d.conn.ReadFromUDP(bytes)
		// Process packet
		d.packetCh <- bytes[0:n]
	}
	d.doneCh <- true
}

func (d *UDPDaemon) ProcessHandler() {
	for {
		d.ProcessPacket(<- d.packetCh)
	}
}

func (d *UDPDaemon) ProcessPacket(bytes []byte) {
	// Process packet
	// ...
	fmt.Println(bytes)
}

func main() {
	fmt.Println("Begin")
	d, err := NewUDPDaemon(12345)
	if err == nil {
		<- d.doneCh
	}
	fmt.Println("Done")
}
