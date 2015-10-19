package main

import (
	"net"
	"runtime"
	"fmt"
)

type PacketStream struct {
	bytes []byte
	prev *PacketStream
}

type UDPDaemon struct {
	conn *net.UDPConn
	k int
	packetCh chan *PacketStream
	doneCh chan bool
}

func NewUDPDaemon(port int, k int) (*UDPDaemon, error) {
	d := &UDPDaemon{}
	d.k = k
	d.packetCh = make(chan *PacketStream)
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
	var prev *PacketStream
	for {
		// Read packet
		bytes := make([]byte, 2000)
		n, _, _ := d.conn.ReadFromUDP(bytes)
		// Process packet
		packet := &PacketStream{bytes[0:n], prev}
		d.packetCh <- packet
		prev = packet
	}
	d.doneCh <- true
}

func (d *UDPDaemon) ProcessHandler() {
	for {
		packet := <- d.packetCh
		d.ProcessPacket(packet)
	}
}

func (d *UDPDaemon) ProcessPacket(packet *PacketStream) {
	// Process using k most recent packets
	// ...
	s := ""
	for i := 0; i < d.k; i++ {
		s += fmt.Sprint(packet.bytes)
		if packet.prev != nil {
			packet = packet.prev
		} else {
			break
		}
	}
	fmt.Println(s)
}

func main() {
	fmt.Println("Begin")
	d, err := NewUDPDaemon(12345, 5)
	if err == nil {
		<- d.doneCh
	}
	fmt.Println("Done")
}
