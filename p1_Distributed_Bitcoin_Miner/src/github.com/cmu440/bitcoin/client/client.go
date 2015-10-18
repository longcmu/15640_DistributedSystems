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
	name = "log_client.txt"
	flag = os.O_RDWR | os.O_CREATE
	perm = os.FileMode(0666)
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce>")
		return
	}

	file, err_o := os.OpenFile(name, flag, perm)
	if err_o != nil {
		file.Close()
		return
	}
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)

	client, err_n := lsp.NewClient(os.Args[1], lsp.NewParams())
	if err_n != nil {
		LOGF.Println(err_n.Error())
		file.Close()
		return
	}

	upper, err_p := strconv.ParseUint(os.Args[3], 10, 64)
	if err_p != nil {
		LOGF.Println(err_p.Error())
	}
	msgD := bitcoin.NewRequest(os.Args[2], 0, upper)
	msgB, err_m := json.Marshal(msgD)
	if err_m != nil {
		LOGF.Println(err_m.Error())
	}
	err_w := client.Write(msgB)
	if err_w != nil {
		printDisconnected()
		LOGF.Println(err_w.Error())
		client.Close()
		file.Close()
		return
	}

	LOGF.Println("sss")
	msgB, err_r := client.Read()
	LOGF.Println(string(msgB))
	if err_r != nil {
		printDisconnected()
		LOGF.Println(err_r.Error())
		file.Close()
	} else {
		m := bitcoin.Message{}
		err_u := json.Unmarshal(msgB, &m)
		if err_u != nil {
			LOGF.Println(err_u.Error())
		}
		printResult(strconv.FormatUint(m.Hash, 10), strconv.FormatUint(m.Nonce, 10))
		client.Close()
		file.Close()
	}
}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
