package paxos

import (
	"encoding/json"
	"errors"
	"github.com/cmu440-F15/paxosapp/rpc/paxosrpc"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

const (
	TimeoutSecond = 15
	RetryInterval = 1
)

// ThreadSafeMap is a wrapper class for map which handles the mutex operation
type ThreadSafeMap struct {
	Value map[string]interface{}
	mutex *sync.Mutex
}

func (tsmap *ThreadSafeMap) Get(key string) (interface{}, bool) {
	tsmap.mutex.Lock()
	defer tsmap.mutex.Unlock()
	value, exist := tsmap.Value[key]
	return value, exist
}

func (tsmap *ThreadSafeMap) Set(key string, value interface{}) {
	tsmap.mutex.Lock()
	defer tsmap.mutex.Unlock()
	tsmap.Value[key] = value
}

/*
 * KeyValue: mapping from key to value
 * KeyN: mapping from key to N which is already accepted
 * KeyNp: mapping from key to the largest N seen so far
 */
type paxosNode struct {
	KeyValue   ThreadSafeMap
	KeyN       ThreadSafeMap
	KeyNp      ThreadSafeMap
	myHostPort string
	hostMap    map[int]string
	cliMap     map[int]*rpc.Client
	numNodes   int
	srvId      int
	numRetries int
}

// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if the node
// could not be started in spite of dialing the other nodes numRetries times.
//
// hostMap is a map from node IDs to their hostports, numNodes is the number
// of nodes in the ring, replace is a flag which indicates whether this node
// is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {
	node := &paxosNode{
		KeyValue:   ThreadSafeMap{make(map[string]interface{}), new(sync.Mutex)},
		KeyN:       ThreadSafeMap{make(map[string]interface{}), new(sync.Mutex)},
		KeyNp:      ThreadSafeMap{make(map[string]interface{}), new(sync.Mutex)},
		myHostPort: myHostPort,
		hostMap:    hostMap,
		cliMap:     make(map[int]*rpc.Client),
		numNodes:   numNodes,
		srvId:      srvId,
		numRetries: numRetries,
	}
	rpc.RegisterName("PaxosNode", paxosrpc.Wrap(node))
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	go http.Serve(listener, nil)

	connectNodesFinishChan := make(chan struct{})
	errorChan := make(chan error)
	go node.waitForAllNodesReady(connectNodesFinishChan, errorChan)
	select {
	case <-connectNodesFinishChan:
		if replace {
			replaceFinishChan := make(chan struct{})
			replaceErrorChan := make(chan error)
			go node.handleReplace(replaceFinishChan, replaceErrorChan)
			select {
			case <-replaceFinishChan:
				return node, nil
			case err := <-replaceErrorChan:
				return nil, err
			}
		}
		return node, nil
	case err := <-errorChan:
		return nil, err
	}
}

// Handle the replacement of a paxos node
func (pn *paxosNode) handleReplace(finishChan chan struct{}, errorChan chan error) {
	args := paxosrpc.ReplaceServerArgs{
		SrvID:    pn.srvId,
		Hostport: pn.myHostPort,
	}
	collectChan := make(chan struct{})
	// call ReplaceServer RPC call to every node
	for _, cli := range pn.cliMap {
		go pn.callReplaceServer(args, cli, collectChan, errorChan)
	}
	for i := 0; i < pn.numNodes; i++ {
		<-collectChan
	}
	// get node data from one of the nodes
	catchupArgs := paxosrpc.ReplaceCatchupArgs{}
	for _, cli := range pn.cliMap {
		var reply paxosrpc.ReplaceCatchupReply
		err := cli.Call("PaxosNode.RecvReplaceCatchup", catchupArgs, &reply)
		if err != nil {
			errorChan <- err
			return
		}
		var newNode paxosNode
		json.Unmarshal(reply.Data, &newNode)
		pn.KeyValue.Value = newNode.KeyValue.Value
		pn.KeyN.Value = newNode.KeyN.Value
		pn.KeyNp.Value = newNode.KeyNp.Value
		break
	}
	finishChan <- struct{}{}
}

// Handle the RPC call to replace servers
func (pn *paxosNode) callReplaceServer(args paxosrpc.ReplaceServerArgs, client *rpc.Client, recvChan chan struct{}, errorChan chan error) {
	var reply paxosrpc.ReplaceServerReply
	err := client.Call("PaxosNode.RecvReplaceServer", args, &reply)
	if err != nil {
		errorChan <- err
	} else {
		recvChan <- struct{}{}
	}
}

// Waiting for all nodes to be ready
func (pn *paxosNode) waitForAllNodesReady(finishChan chan struct{}, errorChan chan error) {
	collectChan := make(chan struct{})
	for srvId, hostPort := range pn.hostMap {
		go pn.contactNode(srvId, hostPort, collectChan, errorChan)
	}
	for i := 0; i < pn.numNodes; i++ {
		<-collectChan
	}
	finishChan <- struct{}{}
}

// contact a paxos node and retry for 5 times
func (pn *paxosNode) contactNode(srvId int, hostPort string, collectChan chan struct{}, errorChan chan error) {
	cli, err := rpc.DialHTTP("tcp", hostPort)
	count := 0
	for err != nil && count < pn.numRetries {
		count++
		time.Sleep(time.Duration(RetryInterval) * time.Second)
		cli, err = rpc.DialHTTP("tcp", hostPort)
	}
	if err != nil {
		errorChan <- errors.New("Failed after retries")
		return
	}
	pn.cliMap[srvId] = cli
	collectChan <- struct{}{}
}

func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	max, exist := pn.KeyN.Get(args.Key)
	if exist {
		maxRound, _ := max.(int)
		round := maxRound / pn.numNodes
		reply.N = (round+1)*pn.numNodes + pn.srvId
	} else {
		reply.N = pn.srvId
	}
	return nil
}

func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	tick := time.Tick(time.Duration(TimeoutSecond) * time.Second)
	resultChan := make(chan interface{})
	errorChan := make(chan error)
	go pn.handlePropose(args, resultChan, errorChan)
	select {
	case <-tick:
		return errors.New("Timeout")
	case result := <-resultChan:
		reply.V = result
		return nil
	case err := <-errorChan:
		return err
	}
}

// Handle the prepare, accept and commit RPC calls
func (pn *paxosNode) handlePropose(args *paxosrpc.ProposeArgs, retChan chan interface{}, errorChan chan error) {
	// Call RecvPrepare for all other nodes
	prepareArgs := &paxosrpc.PrepareArgs{
		Key: args.Key,
		N:   args.N,
	}
	prepareRecvChan := make(chan *paxosrpc.PrepareReply)
	go pn.handlePrepare(prepareArgs, prepareRecvChan, errorChan)
	max_N, accept_V := pn.processPrepareRecvChan(prepareRecvChan, errorChan)

	// if there is no leader or current node is the leader, it could override the value
	leaderNValue, _ := pn.KeyN.Get(args.Key)
	leaderN, _ := leaderNValue.(int)
	leaderId := leaderN % pn.numNodes
	if max_N <= args.N && (accept_V == nil || leaderId == pn.srvId) {
		accept_V = args.V
	}

	// call RecvAccept to all other nodes
	acceptArgs := &paxosrpc.AcceptArgs{
		Key: args.Key,
		N:   args.N,
		V:   accept_V,
	}
	acceptRecvChan := make(chan *paxosrpc.AcceptReply)
	go pn.handleAccept(acceptArgs, acceptRecvChan, errorChan)
	pn.processAcceptRecvChan(acceptRecvChan, errorChan)

	// call RecvCommit to all other nodes
	commitArgs := &paxosrpc.CommitArgs{
		Key: args.Key,
		V:   accept_V,
	}
	commitRecvChan := make(chan *paxosrpc.CommitReply)
	go pn.handleCommit(commitArgs, commitRecvChan, errorChan)
	pn.processCommitRecvChan(commitRecvChan)

	retChan <- accept_V
}

// wait for all commit messages
func (pn *paxosNode) processCommitRecvChan(recvChan chan *paxosrpc.CommitReply) {
	for i := 0; i < pn.numNodes; i++ {
		<-recvChan
	}
}

// handle all the RecvCommit RPC calls
func (pn *paxosNode) handleCommit(args *paxosrpc.CommitArgs, recvChan chan *paxosrpc.CommitReply, errorChan chan error) {
	for _, cli := range pn.cliMap {
		go pn.callCommit(cli, args, recvChan, errorChan)
	}
}

// make RecvCommit RPC Call to a paxos node
func (pn *paxosNode) callCommit(client *rpc.Client, args *paxosrpc.CommitArgs, recvChan chan *paxosrpc.CommitReply, errorChan chan error) {
	var reply *paxosrpc.CommitReply
	err := client.Call("PaxosNode.RecvCommit", args, &reply)
	if err != nil {
		errorChan <- err
	} else {
		recvChan <- reply
	}
}

// wait for all accept responses and process ACKs and NAKs
func (pn *paxosNode) processAcceptRecvChan(recvChan chan *paxosrpc.AcceptReply, errorChan chan error) {
	ack := 0
	for i := 0; i < pn.numNodes; i++ {
		reply := <-recvChan
		if reply.Status == paxosrpc.OK {
			ack++
		}
	}
	if ack < pn.numNodes/2+1 {
		errorChan <- errors.New("Accept didn't get majority vote")
	}
}

// handle all the RecvAccept RPC calls
func (pn *paxosNode) handleAccept(args *paxosrpc.AcceptArgs, recvChan chan *paxosrpc.AcceptReply, errorChan chan error) {
	for _, cli := range pn.cliMap {
		go pn.callAccept(cli, args, recvChan, errorChan)
	}
}

// make RecvAccept RPC Call to a paxos node
func (pn *paxosNode) callAccept(client *rpc.Client, args *paxosrpc.AcceptArgs, recvChan chan *paxosrpc.AcceptReply, errorChan chan error) {
	var reply *paxosrpc.AcceptReply
	err := client.Call("PaxosNode.RecvAccept", args, &reply)
	if err != nil {
		errorChan <- err
	} else {
		recvChan <- reply
	}
}

// wait for all prepare responses and process ACKs and NAKs
func (pn *paxosNode) processPrepareRecvChan(recvChan chan *paxosrpc.PrepareReply, errorChan chan error) (int, interface{}) {
	ack := 0
	maxN := -1
	var vMaxN interface{}
	for i := 0; i < pn.numNodes; i++ {
		reply := <-recvChan
		if reply.Status == paxosrpc.OK {
			ack++
			// find the max sequence number and its corresponding value
			if reply.N_a > maxN {
				maxN = reply.N_a
				vMaxN = reply.V_a
			}
		}
	}
	if ack < pn.numNodes/2+1 {
		errorChan <- errors.New("Prepare didn't get majority vote")
		return -1, nil
	}
	return maxN, vMaxN
}

// handle all the RecvPrepare RPC calls
func (pn *paxosNode) handlePrepare(args *paxosrpc.PrepareArgs, recvChan chan *paxosrpc.PrepareReply, errorChan chan error) {
	for _, cli := range pn.cliMap {
		go pn.callPrepare(cli, args, recvChan, errorChan)
	}
}

// make RecvPrepare RPC Call to a paxos node
func (pn *paxosNode) callPrepare(client *rpc.Client, args *paxosrpc.PrepareArgs, recvChan chan *paxosrpc.PrepareReply, errorChan chan error) {
	var reply *paxosrpc.PrepareReply
	err := client.Call("PaxosNode.RecvPrepare", args, &reply)
	if err != nil {
		errorChan <- err
	} else {
		recvChan <- reply
	}
}

func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	key := args.Key
	if value, ok := pn.KeyValue.Get(key); !ok {
		reply.Status = paxosrpc.KeyNotFound
	} else {
		reply.Status = paxosrpc.KeyFound
		reply.V = value
	}

	return nil
}

func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	n := args.N
	key := args.Key
	NpValue, existKey := pn.KeyNp.Get(key)
	Np, _ := NpValue.(int)
	if !existKey || n > Np {
		pn.KeyNp.Set(key, n)
		NValue, existN := pn.KeyN.Get(key)
		N, _ := NValue.(int)
		V, existV := pn.KeyValue.Get(key)
		if existN && existV {
			reply.N_a = N
			reply.V_a = V
		} else {
			reply.N_a = -1
		}
		reply.Status = paxosrpc.OK
	} else {
		reply.Status = paxosrpc.Reject
	}

	return nil
}

func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	n := args.N
	key := args.Key
	value := args.V
	NpValue, exist := pn.KeyNp.Get(key)
	Np, _ := NpValue.(int)
	if exist && n >= Np {
		pn.KeyN.Set(key, n)
		pn.KeyValue.Set(key, value)
		reply.Status = paxosrpc.OK
	} else {
		reply.Status = paxosrpc.Reject
	}

	return nil
}

func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	key := args.Key
	value := args.V
	pn.KeyValue.Set(key, value)

	return nil
}

func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	pn.hostMap[args.SrvID] = args.Hostport
	cli, err := rpc.DialHTTP("tcp", args.Hostport)
	count := 0
	for err != nil && count < pn.numRetries {
		count++
		time.Sleep(time.Duration(1) * time.Second)
		cli, err = rpc.DialHTTP("tcp", args.Hostport)
	}
	if err != nil {
		return errors.New("Failed after retries")
	}
	pn.cliMap[args.SrvID] = cli
	return nil
}

func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	b, err := json.Marshal(pn)
	if err != nil {
		println("error:", err)
	}
	reply.Data = b

	return nil
}
