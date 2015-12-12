package controllers

import (
	"fmt"
	"github.com/cmu440-F15/paxosapp/rpc/paxosrpc"
	"net/http"
	"net/rpc"
	"strconv"
	"time"
)

var (
	// pre-registered server Id and hostport
	hostMap = map[int]string{
		0: "localhost:9001",
		1: "localhost:9002",
		2: "localhost:9003",
	}
)

type UserController struct {
	round   int
	clients []*rpc.Client
}

// Return a new controller until connecting all paxos nodes
func NewUserController() (*UserController, error) {
	uc := &UserController{
		round:   1,
		clients: make([]*rpc.Client, 0),
	}
	for nodeId, hostPort := range hostMap {
		uc.contactNode(nodeId, hostPort)
	}
	return uc, nil
}

// Contact a specific paxos node
func (uc *UserController) contactNode(nodeId int, hostPort string) error {
	cli, err := rpc.DialHTTP("tcp", hostPort)
	// retry up to five times
	count := 0
	for err != nil && count < 5 {
		count++
		time.Sleep(time.Duration(1) * time.Second)
		cli, err = rpc.DialHTTP("tcp", hostPort)
	}
	if err != nil {
		return err
	}
	uc.clients = append(uc.clients, cli)
	return nil
}

// Handle propose RPC call to corresponding paxos node including getnextproposalnumber RPC call
func (uc *UserController) Propose(w http.ResponseWriter, r *http.Request) {
	round := r.FormValue("key")
	nodeId, _ := strconv.Atoi(r.FormValue("id"))
	value, _ := strconv.Atoi(r.FormValue("value"))

	proposeVal := fmt.Sprintf("%d %d", nodeId, value)
	nextNumberArgs := &paxosrpc.ProposalNumberArgs{
		Key: round,
	}
	var nextNumberReply paxosrpc.ProposalNumberReply
	uc.clients[nodeId].Call("PaxosNode.GetNextProposalNumber", nextNumberArgs, &nextNumberReply)
	proposeArgs := &paxosrpc.ProposeArgs{
		N:   nextNumberReply.N,
		Key: round,
		V:   proposeVal,
	}
	var proposeReply paxosrpc.ProposeReply
	uc.clients[nodeId].Call("PaxosNode.Propose", proposeArgs, &proposeReply)
	str, _ := proposeReply.V.(string)
	if str == proposeVal {
		uc.round++
	}
	fmt.Fprintln(w, proposeReply.V)
}

// Handle getvalue RPC call to corresponding paxos node
func (uc *UserController) GetValue(w http.ResponseWriter, r *http.Request) {
	round := r.URL.Query().Get("key")
	args := &paxosrpc.GetValueArgs{
		Key: round,
	}
	var response interface{}
	for _, cli := range uc.clients {
		var reply paxosrpc.GetValueReply
		cli.Call("PaxosNode.GetValue", args, &reply)
		response = reply.V
		break
	}
	fmt.Fprintln(w, response)
}

// Return the current round of the game
func (uc *UserController) GetRound(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, uc.round)
}
