// Andrew ID: longh
// Name: Long He
// go version: 1.42

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

const (
	name = "log_miner.txt"
	flag = os.O_RDWR | os.O_CREATE
	perm = os.FileMode(0666)
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}

	file, err_o := os.OpenFile(name, flag, perm)
	if err_o != nil {
		file.Close()
		return
	}
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)

	miner, err_n := lsp.NewClient(os.Args[1], lsp.NewParams())
	if err_n != nil {
		LOGF.Println(err_n.Error())
		file.Close()
		return
	}

	msgD := bitcoin.NewJoin()
	msgB, err_m := json.Marshal(msgD)
	if err_m != nil {
		LOGF.Println(err_m.Error())
	}
	err_w := miner.Write(msgB)
	if err_w != nil {
		LOGF.Println(err_w.Error())
		miner.Close()
		file.Close()
		return
	}

	for {
		msgB, err_r := miner.Read()
		if err_r != nil {
			LOGF.Println(err_r.Error())
			file.Close()
			return
		}
		m := bitcoin.Message{}
		err_u := json.Unmarshal(msgB, &m)
		if err_u != nil {
			LOGF.Println(err_u.Error())
		}

		msg := m.Data
		lower := m.Lower
		upper := m.Upper
		var tmp, hash, nonce uint64
		hash = 1<<64 - 1
		hash = 1<<64 - 1
		for i := lower; i <= upper; i++ {
			tmp = bitcoin.Hash(msg, i)
			if tmp < hash {
				hash = tmp
				nonce = i
			} else if tmp == hash {
				if i < nonce {
					nonce = i
				}
			}
		}

		msgB, err_m := json.Marshal(bitcoin.NewResult(hash, nonce))
		if err_m != nil {
			LOGF.Println(err_m.Error())
		}
		LOGF.Println(string(msgB))
		err_w := miner.Write(msgB)
		if err_w != nil {
			LOGF.Println(err_w.Error())
			miner.Close()
			file.Close()
			return
		}
	}
}
