package libstore

import (
	"errors"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type content struct {
	valid        bool
	value        interface{}
	validSeconds int
	startTime    time.Time
}

type count struct {
	counter   int
	startTime time.Time
}

type server struct {
	NodeID uint32
	cli    *rpc.Client
}

type libstore struct {
	mutex sync.Mutex

	master     *rpc.Client
	hostPort   string
	mode       LeaseMode
	cache      map[string]interface{}
	queryCount map[string]interface{}
	servers    []server
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	// dial the master
	master, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	libstore := &libstore{
		mutex:      sync.Mutex{},
		master:     master,
		hostPort:   myHostPort,
		mode:       mode,
		cache:      make(map[string]interface{}),
		queryCount: make(map[string]interface{}),
		servers:    make([]server, 0),
	}

	// get all servers
	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	for i := 0; i < 6; i++ { // retry up to 5 times
		if err := libstore.master.Call("StorageServer.GetServers", args, &reply); err != nil {
			return nil, err
		} else {
			if reply.Status == storagerpc.OK {
				for _, s := range reply.Servers {
					// dial this slave
					cli, err := rpc.DialHTTP("tcp", s.HostPort)
					if err != nil {
						return nil, err
					}
					libstore.servers = append(libstore.servers, server{s.NodeID, cli})
				}
				sort.Sort(ByNodeID(libstore.servers))
				break
			} else { // not ready
				time.Sleep(time.Second)
			}
		}
	}

	// Wrap the RemoteLeaseCallbacks before registering it for RPC.
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	if err != nil {
		return nil, err
	}

	// update cache and queryCount
	go func() {
		for {
			select {
			case <-time.After(time.Second * storagerpc.QueryCacheSeconds):
				libstore.UpdateLeases()
			}
		}
	}()

	return libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	ls.UpdateLeases()
	//ls.mutex.Lock()
	//defer ls.mutex.Unlock()
	// count the query
	if c, ok := ls.queryCount[key].(count); ok {
		ls.queryCount[key] = count{c.counter + 1, time.Now()}
	} else {
		ls.queryCount[key] = count{1, time.Now()}
	}

	// prepare params
	args := &storagerpc.GetArgs{key, false, ls.hostPort}
	if ls.mode == Always {
		args.WantLease = true
	}
	if len(ls.hostPort) == 0 {
		args.WantLease = false
	}
	var reply storagerpc.GetReply

	// three cache cases:
	cont, ok := ls.cache[key].(content)
	if ok && cont.valid { // 1. cached and valid
		return cont.value.(string), nil
	} else if !ok { // 2. not cached but want
		c, _ := ls.queryCount[key].(count)
		if c.counter >= storagerpc.QueryCacheThresh {
			args.WantLease = true
			if len(ls.hostPort) == 0 || ls.mode == Never {
				args.WantLease = false
			}
			if err := ls.Route(key).Call("StorageServer.Get", args, &reply); err != nil {
				return "", err
			}
			if reply.Status == storagerpc.KeyNotFound {
				return "", errors.New("key not found")
			} else if reply.Status == storagerpc.WrongServer {
				return "", errors.New("wrong server")
			}

			if reply.Lease.Granted {
				ls.cache[key] = content{true, reply.Value, reply.Lease.ValidSeconds, time.Now()}
			}
			return reply.Value, nil
		}
	}

	// 3. else
	if err := ls.Route(key).Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	}
	if reply.Status == storagerpc.KeyNotFound {
		return "", errors.New("key not found")
	} else if reply.Status == storagerpc.WrongServer {
		return "", errors.New("wrong server")
	}

	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{key, value}
	var reply storagerpc.PutReply

	if err := ls.Route(key).Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK")
	}

	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{key}
	var reply storagerpc.DeleteReply
	if err := ls.Route(key).Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK")
	}

	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.UpdateLeases()
	//ls.mutex.Lock()
	//defer ls.mutex.Unlock()
	// count the query
	if c, ok := ls.queryCount[key].(count); ok {
		ls.queryCount[key] = count{c.counter + 1, time.Now()}
	} else {
		ls.queryCount[key] = count{1, time.Now()}
	}

	// prepare params
	args := &storagerpc.GetArgs{key, false, ls.hostPort}
	if ls.mode == Always {
		args.WantLease = true
	}
	if len(ls.hostPort) == 0 {
		args.WantLease = false
	}
	var reply storagerpc.GetListReply

	// there cache cases:
	cont, ok := ls.cache[key].(content)
	if ok && cont.valid { // 1. cached and valid
		return cont.value.([]string), nil
	} else if !ok { // 2. not cached but want
		c, _ := ls.queryCount[key].(count)
		if c.counter >= storagerpc.QueryCacheThresh {
			args.WantLease = true
			if len(ls.hostPort) == 0 || ls.mode == Never {
				args.WantLease = false
			}
			if err := ls.Route(key).Call("StorageServer.GetList", args, &reply); err != nil {
				return nil, err
			}
			if reply.Status == storagerpc.KeyNotFound {
				return []string{}, errors.New("key not found")
			} else if reply.Status == storagerpc.WrongServer {
				return nil, errors.New("wrong server")
			}

			if reply.Lease.Granted {
				ls.cache[key] = content{true, reply.Value, reply.Lease.ValidSeconds, time.Now()}
			}

			return reply.Value, nil
		}
	}

	// 3. else
	if err := ls.Route(key).Call("StorageServer.GetList", args, &reply); err != nil {
		return nil, err
	}
	if reply.Status == storagerpc.KeyNotFound {
		return []string{}, errors.New("key not found")
	} else if reply.Status == storagerpc.WrongServer {
		return nil, errors.New("wrong server")
	}

	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{key, removeItem}
	var reply storagerpc.PutReply
	if err := ls.Route(key).Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK")
	}

	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{key, newItem}
	var reply storagerpc.PutReply
	if err := ls.Route(key).Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		return errors.New("Status not OK")
	}

	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	//ls.mutex.Lock()
	//defer ls.mutex.Unlock()
	if _, ok := ls.cache[args.Key]; !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		delete(ls.cache, args.Key)
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ls *libstore) UpdateLeases() {
	//ls.mutex.Lock()
	//defer ls.mutex.Unlock()
	for key, c := range ls.queryCount {
		endTime := c.(count).startTime.Add(time.Second * time.Duration(storagerpc.QueryCacheSeconds))
		if endTime.Before(time.Now()) {
			delete(ls.queryCount, key)
		}
	}
	for key, c := range ls.cache {
		endTime := c.(content).startTime.Add(time.Second * time.Duration(c.(content).validSeconds))
		if endTime.Before(time.Now()) {
			delete(ls.cache, key)
		}
	}
}

func (ls *libstore) Route(key string) *rpc.Client {
	hash := StoreHash(key)
	for _, s := range ls.servers {
		if s.NodeID >= hash {
			return s.cli
		}
	}
	return ls.servers[0].cli
}

// sort servers by NodeID
type ByNodeID []server

func (s ByNodeID) Len() int {
	return len(s)
}
func (s ByNodeID) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByNodeID) Less(i, j int) bool {
	return s[i].NodeID < s[j].NodeID
}
