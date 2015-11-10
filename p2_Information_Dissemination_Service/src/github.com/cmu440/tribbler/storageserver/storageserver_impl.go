package storageserver

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"container/list"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"math"
	"strings"
	"sync"
	"time"
)

type storageServer struct {
	masterServerHostPort string
	numNodes             int
	port                 int
	nodeID               uint32
	keyValue             map[string]string
	keyList              map[string][]string
	Servers              map[uint32]storagerpc.Node
	serverList           []storagerpc.Node
	leaseMap             map[string]*list.List
	clientMap            map[string]*rpc.Client
	mutex                *sync.Mutex
	mutexMap             map[string]*sync.Mutex
}

type lease struct {
	hostPort  string
	startTime time.Time
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	ss := &storageServer{
		masterServerHostPort: masterServerHostPort,
		numNodes:             numNodes,
		port:                 port,
		nodeID:               nodeID,
		keyValue:             make(map[string]string),
		keyList:              make(map[string][]string),
		Servers:              make(map[uint32]storagerpc.Node, 0),
		serverList:           make([]storagerpc.Node, numNodes),
		leaseMap:             make(map[string]*list.List),
		clientMap:            make(map[string]*rpc.Client),
		mutex:                new(sync.Mutex),
		mutexMap:             make(map[string]*sync.Mutex),
	}

	serverInfo := storagerpc.Node{
		HostPort: fmt.Sprintf(":%d", ss.port),
		NodeID:   ss.nodeID,
	}

	if len(ss.masterServerHostPort) == 0 {
		ss.Servers[ss.nodeID] = serverInfo

		if numNodes == 1 {
			ss.serverList[0] = serverInfo
		}
		// Wrap the storageServer before registering it for RPC.
		rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
		rpc.RegisterName("MasterServer", storagerpc.Wrap(ss))

		// Setup the HTTP handler that will server incoming RPCs and
		// serve requests in a background goroutine.
		rpc.HandleHTTP()

		// Create the server socket that will listen for incoming RPCs.
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return nil, err
		}
		go http.Serve(listener, nil)

		for len(ss.Servers) < numNodes {
			time.Sleep(time.Second)
		}
	} else {
		cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
		for err != nil {
			time.Sleep(time.Second)
			cli, err = rpc.DialHTTP("tcp", masterServerHostPort)
		}
		args := &storagerpc.RegisterArgs{
			ServerInfo: serverInfo,
		}
		var reply storagerpc.RegisterReply
		for {
			if err := cli.Call("MasterServer.RegisterServer", args, &reply); err == nil {
				if reply.Status == storagerpc.OK {
					break
				}
			}
			time.Sleep(time.Second)
		}
		ss.serverList = reply.Servers
		rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
		rpc.HandleHTTP()

		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return nil, err
		}
		go http.Serve(listener, nil)
	}

	go ss.timeLeases()
	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()
	if _, ok := ss.Servers[args.ServerInfo.NodeID]; !ok {
		ss.Servers[args.ServerInfo.NodeID] = args.ServerInfo
	}
	if len(ss.Servers) < ss.numNodes {
		reply.Status = storagerpc.NotReady
	} else {
		i := 0
		for _, node := range ss.Servers {
			ss.serverList[i] = node
			i++
		}
		reply.Servers = ss.serverList
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if len(ss.Servers) < ss.numNodes {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Status = storagerpc.OK
		reply.Servers = ss.serverList
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.isInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.mutex.Lock()
	keyMutex, exist := ss.mutexMap[args.Key]
	if !exist {
		keyMutex = new(sync.Mutex)
		ss.mutexMap[args.Key] = keyMutex
	}
	ss.mutex.Unlock()

	keyMutex.Lock()
	defer keyMutex.Unlock()

	ss.GrantLease(args)

	value, ok := ss.keyValue[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = value
		if args.WantLease {
			reply.Lease = storagerpc.Lease{true, storagerpc.LeaseSeconds}
		}
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !ss.isInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.mutex.Lock()
	keyMutex, exist := ss.mutexMap[args.Key]
	if !exist {
		keyMutex = new(sync.Mutex)
		ss.mutexMap[args.Key] = keyMutex
	}
	ss.mutex.Unlock()

	keyMutex.Lock()
	defer keyMutex.Unlock()

	ss.RevokeLease(args.Key)

	_, ok := ss.keyValue[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		delete(ss.keyValue, args.Key)
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.isInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.mutex.Lock()
	keyMutex, exist := ss.mutexMap[args.Key]
	if !exist {
		keyMutex = new(sync.Mutex)
		ss.mutexMap[args.Key] = keyMutex
	}
	ss.mutex.Unlock()

	keyMutex.Lock()
	defer keyMutex.Unlock()

	ss.GrantLease(args)

	list, ok := ss.keyList[args.Key]
	if !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = list
		if args.WantLease {
			reply.Lease = storagerpc.Lease{true, storagerpc.LeaseSeconds}
		}
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.mutex.Lock()
	keyMutex, exist := ss.mutexMap[args.Key]
	if !exist {
		keyMutex = new(sync.Mutex)
		ss.mutexMap[args.Key] = keyMutex
	}
	ss.mutex.Unlock()

	keyMutex.Lock()
	defer keyMutex.Unlock()

	ss.RevokeLease(args.Key)

	reply.Status = storagerpc.OK
	ss.keyValue[args.Key] = args.Value
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.mutex.Lock()
	keyMutex, exist := ss.mutexMap[args.Key]
	if !exist {
		keyMutex = new(sync.Mutex)
		ss.mutexMap[args.Key] = keyMutex
	}
	ss.mutex.Unlock()

	keyMutex.Lock()
	defer keyMutex.Unlock()

	ss.RevokeLease(args.Key)

	list, ok := ss.keyList[args.Key]
	if !ok {
		reply.Status = storagerpc.OK
		list := make([]string, 0)
		ss.keyList[args.Key] = append(list, args.Value)
	} else {
		for _, v := range list {
			if v == args.Value {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
		reply.Status = storagerpc.OK
		ss.keyList[args.Key] = append(list, args.Value)
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.mutex.Lock()
	keyMutex, exist := ss.mutexMap[args.Key]
	if !exist {
		keyMutex = new(sync.Mutex)
		ss.mutexMap[args.Key] = keyMutex
	}
	ss.mutex.Unlock()

	keyMutex.Lock()
	defer keyMutex.Unlock()

	ss.RevokeLease(args.Key)

	list, ok := ss.keyList[args.Key]
	if !ok {
		reply.Status = storagerpc.ItemNotFound
	} else {
		for i, v := range list {
			if v == args.Value {
				ss.keyList[args.Key] = append(list[:i], list[i+1:]...)
				reply.Status = storagerpc.OK
				return nil
			}
		}
		reply.Status = storagerpc.ItemNotFound
	}
	return nil
}

func (ss *storageServer) GrantLease(args *storagerpc.GetArgs) {
	if args.WantLease {
		leaseList, exist := ss.leaseMap[args.Key]
		if !exist {
			leaseList = new(list.List)
		}
		leaseList.PushBack(lease{
			hostPort:  args.HostPort,
			startTime: time.Now(),
		})
		ss.leaseMap[args.Key] = leaseList
	}
}

func (ss *storageServer) RevokeLease(key string) {
	_, exist := ss.leaseMap[key]
	if exist {
		leaseList := ss.leaseMap[key]
		revokeChan := make(chan struct{})
		collectChan := make(chan struct{})
		go ss.collectRevoke(key, revokeChan, collectChan)
		for e := leaseList.Front(); e != nil; e = e.Next() {
			lease := e.Value.(lease)
			go ss.revokeLease(lease.hostPort, key, revokeChan)
		}
		<-collectChan
		delete(ss.leaseMap, key)
	}
}

func (ss *storageServer) revokeLease(hostPort string, key string, revokeChan chan struct{}) {
	_, exist := ss.clientMap[hostPort]
	if !exist {
		cli, err := rpc.DialHTTP("tcp", hostPort)
		if err != nil {
			return
		}
		ss.clientMap[hostPort] = cli
	}
	cli := ss.clientMap[hostPort]
	args := &storagerpc.RevokeLeaseArgs{
		Key: key,
	}
	var reply storagerpc.RevokeLeaseReply
	err := cli.Call("LeaseCallbacks.RevokeLease", args, &reply)
	if err != nil {
		return
	}
	revokeChan <- struct{}{}
}

func (ss *storageServer) collectRevoke(key string, revokeChan chan struct{}, collectChan chan struct{}) {
	count := 0
	tick := time.Tick(time.Duration(500) * time.Millisecond)
	for {
		select {
		case <-revokeChan:
			count++
			leaseList := ss.leaseMap[key]
			if count == leaseList.Len() {
				collectChan <- struct{}{}
				return
			}
		case <-tick:
			leaseList := ss.leaseMap[key]
			if count == leaseList.Len() {
				collectChan <- struct{}{}
				return
			}
		}
	}
}

func (ss *storageServer) isInRange(key string) bool {
	hashVal := libstore.StoreHash(strings.Split(key, ":")[0])
	var minNodeId, targetNodeId uint32
	minNodeId = math.MaxUint32
	targetNodeId = math.MaxUint32

	for i := 0; i < len(ss.serverList); i++ {
		nodeId := ss.serverList[i].NodeID
		if nodeId >= hashVal && nodeId < targetNodeId {
			targetNodeId = nodeId
		}
		if nodeId < minNodeId {
			minNodeId = nodeId
		}
	}
	if targetNodeId == math.MaxUint32 {
		targetNodeId = minNodeId
	}
	if targetNodeId == ss.nodeID {
		return true
	} else {
		return false
	}
}

func (ss *storageServer) timeLeases() {
	expiration := time.Duration(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds) * time.Second
	tick := time.Tick(time.Duration(500) * time.Millisecond)
	for {
		select {
		case <-tick:
			for _, leaseList := range ss.leaseMap {
				for e := leaseList.Front(); e != nil; e = e.Next() {
					lease := e.Value.(lease)
					if time.Since(lease.startTime) > expiration {
						leaseList.Remove(e)
					} else {
						break
					}
				}
			}
		}
	}
}
