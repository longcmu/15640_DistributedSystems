// Andrew ID: longh
// Name: Long He
// go version: 1.42

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

const (
	name = "log_server.txt"
	flag = os.O_RDWR | os.O_CREATE
	perm = os.FileMode(0666)
)

type job struct {
	clientID int
	msg      bitcoin.Message
}

type Scheduler struct {
	clients        map[int]bool
	jobs           chan job
	miners         map[int]bitcoin.Message // minerID -> its task
	preMiners      map[int]bool            // to become miners
	jobDone        chan int                // block until finish
	awaitingBlocks map[int]bitcoin.Message
	hash           uint64
	nonce          uint64

	curJob  job
	running bool
	giveUp  bool
}

func NewScheduler() Scheduler {
	return Scheduler{
		clients:        make(map[int]bool),
		jobs:           make(chan job, 100),
		miners:         make(map[int]bitcoin.Message),
		preMiners:      make(map[int]bool),  // to become miners
		jobDone:        make(chan int, 100), // block until finish
		awaitingBlocks: make(map[int]bitcoin.Message),
		hash:           1<<64 - 1,
		nonce:          1<<64 - 1,
		running:        false,
		giveUp:         false,
	}

}

func adjustMiners(scheduler Scheduler) {
	for k, _ := range scheduler.preMiners {
		scheduler.miners[k] = bitcoin.Message{}
		delete(scheduler.preMiners, k)
	}
}

func main() {
	// get args
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}
	// write log
	file, err_o := os.OpenFile(name, flag, perm)
	if err_o != nil {
		file.Close()
		return
	}
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	port, err_a := strconv.Atoi(os.Args[1])
	if err_a != nil {
		LOGF.Println(err_a.Error())
	}
	LOGF.Println("start log")
	// start the server
	server, err_n := lsp.NewServer(port, lsp.NewParams())
	if err_n != nil {
		LOGF.Println(err_n.Error())
		file.Close()
		return
	}
	// initiate the scheduler
	scheduler := NewScheduler()
	LOGF.Println("new scheduler")
	for {
		select {
		// current job is done
		case <-scheduler.jobDone:
			LOGF.Println("jobDone")
			if !scheduler.giveUp {
				msgD := bitcoin.NewResult(scheduler.hash, scheduler.nonce)
				msgB, err_m := json.Marshal(msgD)
				if err_m != nil {
					LOGF.Println(err_m.Error())
				}
				err_w := server.Write(scheduler.curJob.clientID, msgB)
				if err_w != nil {
					LOGF.Println(err_w.Error())
				}
				delete(scheduler.clients, scheduler.curJob.clientID)
			}
			scheduler.running = false
			scheduler.giveUp = false
			// adjust current miners team
			if !scheduler.running && len(scheduler.preMiners) != 0 {
				LOGF.Println("adjust current miners team")
				adjustMiners(scheduler)
			}
		default:
			// keep reading new messages
			id, payload, err_r := server.Read()
			LOGF.Println(string(payload))
			if err_r != nil {
				LOGF.Println(err_r.Error())
				mn, ok_m := scheduler.miners[id]
				if ok_m { // lost a miner
					delete(scheduler.miners, id)
					scheduler.awaitingBlocks[id] = mn
					// assign failed block jobs to awaiting miners
					if scheduler.running {
						for id_a, ab := range scheduler.awaitingBlocks {
							if len(scheduler.preMiners) == 0 {
								break
							}
							for id_p, _ := range scheduler.preMiners {
								msgB := bitcoin.NewRequest(ab.Data, ab.Lower, ab.Upper)
								msgD, err_m := json.Marshal(msgB)
								if err_m != nil {
									LOGF.Println(err_m.Error())
								}
								err_w := server.Write(id_p, msgD)
								if err_w != nil {
									LOGF.Println(err_w.Error())
								}
								delete(scheduler.preMiners, id_p)
								delete(scheduler.awaitingBlocks, id_a)
							}
						}
					}
				}
				_, ok_p := scheduler.preMiners[id]
				if ok_p { // lost a preminer
					delete(scheduler.preMiners, id)
				}
				_, ok_c := scheduler.clients[id]
				if ok_c { // lost current client
					delete(scheduler.clients, id)
					scheduler.giveUp = true
				}
			} else {
				m := bitcoin.Message{}
				err_u := json.Unmarshal(payload, &m)
				if err_u != nil {
					LOGF.Println(err_u.Error())
				}
				LOGF.Println(string(payload))
				switch m.Type {
				case bitcoin.Result:
					LOGF.Println("get the result")
					hash := m.Hash
					nonce := m.Nonce
					LOGF.Println(hash)
					LOGF.Println(nonce)
					LOGF.Println(scheduler.hash)
					if hash < scheduler.hash {
						LOGF.Println("update the result")
						scheduler.hash = hash
						scheduler.nonce = nonce
					} else if hash == scheduler.hash {
						if nonce < scheduler.nonce {
							scheduler.nonce = nonce
						}
					}
					// this miner fulfill its task
					delete(scheduler.miners, id)
					scheduler.preMiners[id] = true
					if len(scheduler.miners) == 0 {
						scheduler.jobDone <- 1
					}
					LOGF.Println(len(scheduler.miners))
				case bitcoin.Request: // new requests come in
					LOGF.Println("request")
					_, ok := scheduler.clients[id]
					if !ok {
						scheduler.clients[id] = true
					}
					scheduler.jobs <- job{id, m}
					// get a new job
					LOGF.Println(len(scheduler.jobs))
					LOGF.Println(len(scheduler.miners))
					if !scheduler.running && len(scheduler.jobs) != 0 && len(scheduler.miners) != 0 {
						scheduler.curJob = <-scheduler.jobs

						lower, _ := strconv.Atoi(strconv.FormatUint(scheduler.curJob.msg.Lower, 10))
						LOGF.Println("lower")
						LOGF.Println(lower)
						upper, _ := strconv.Atoi(strconv.FormatUint(scheduler.curJob.msg.Upper, 10))
						LOGF.Println("upper")
						LOGF.Println(upper)
						data := scheduler.curJob.msg.Data
						blockSize := (upper - lower) / len(scheduler.miners)
						blockNum := len(scheduler.miners)
						count := 0
						// split the job into block jobs
						LOGF.Println("split the job into block jobs")
						for k, _ := range scheduler.miners {
							if count == blockNum-1 {
								scheduler.miners[k] = bitcoin.Message{Data: data, Lower: uint64(lower + blockSize*count), Upper: uint64(upper)}
								LOGF.Println(int(lower) + blockSize*count)
								LOGF.Println(upper)
								break
							}
							scheduler.miners[k] = bitcoin.Message{Data: data, Lower: uint64(lower + blockSize*count), Upper: uint64(lower + blockSize*(count+1))}
							LOGF.Println(lower + blockSize*count)
							LOGF.Println(lower + blockSize*(count+1))
							count++
						}
						// assign the tasks
						LOGF.Println("assign the tasks")
						for id, m := range scheduler.miners {
							msgB := bitcoin.NewRequest(m.Data, m.Lower, m.Upper)
							msgD, err_m := json.Marshal(msgB)
							if err_m != nil {
								LOGF.Println(err_m.Error())
							}
							err_w := server.Write(id, msgD)
							if err_w != nil {
								LOGF.Println(err_w.Error())
							}
						}

						scheduler.running = true
					}

				case bitcoin.Join: // new miners come in
					LOGF.Println("join")
					_, ok := scheduler.miners[id]
					if !ok {
						scheduler.preMiners[id] = true
					}
					// adjust current miners team
					if !scheduler.running && len(scheduler.preMiners) != 0 {
						LOGF.Println("adjust current miners team")
						adjustMiners(scheduler)
					}
					if !scheduler.running && len(scheduler.jobs) != 0 && len(scheduler.miners) != 0 {
						scheduler.curJob = <-scheduler.jobs

						lower := scheduler.curJob.msg.Lower
						upper := scheduler.curJob.msg.Upper
						data := scheduler.curJob.msg.Data
						blockSize := (lower - upper) / uint64(len(scheduler.miners))
						blockNum := uint64(len(scheduler.miners))
						count := uint64(0)
						// split the job into block jobs
						LOGF.Println("split the job into block jobs")
						for k, _ := range scheduler.miners {
							if count == blockNum-1 {
								scheduler.miners[k] = bitcoin.Message{Data: data, Lower: lower + blockSize*count, Upper: upper}
								LOGF.Println(lower + blockSize*count)
								LOGF.Println(upper)
								break
							}
							scheduler.miners[k] = bitcoin.Message{Data: data, Lower: lower + blockSize*count, Upper: lower + blockSize*(count+1)}
							LOGF.Println(lower + blockSize*count)
							LOGF.Println(lower + blockSize*(count+1))
							count++
						}
						// assign the tasks
						LOGF.Println("assign the tasks")
						for id, m := range scheduler.miners {
							msgB := bitcoin.NewRequest(m.Data, m.Lower, m.Upper)
							msgD, err_m := json.Marshal(msgB)
							if err_m != nil {
								LOGF.Println(err_m.Error())
							}
							err_w := server.Write(id, msgD)
							if err_w != nil {
								LOGF.Println(err_w.Error())
							}
						}

						scheduler.running = true
					}
				}
			}
		}
	}
}
