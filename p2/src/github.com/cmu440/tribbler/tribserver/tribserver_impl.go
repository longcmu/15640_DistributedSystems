package tribserver

import (
	"net"
	"net/http"
	"net/rpc"
	"sort"

	"encoding/json"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"time"
)

type tribServer struct {
	libstore libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	libstore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if err != nil {
		return nil, err
	}
	tribServer := &tribServer{
		libstore: libstore,
	}

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return tribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	if !ts.UserExist(args.UserID) {
		if err := ts.libstore.Put(util.FormatUserKey(args.UserID), args.UserID); err != nil {
			return err
		}
		reply.Status = tribrpc.OK
		return nil
	}
	reply.Status = tribrpc.Exists
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	if ts.UserExist(args.UserID) {
		if ts.UserExist(args.TargetUserID) {
			if err := ts.libstore.AppendToList(util.FormatSubListKey(args.UserID), args.TargetUserID); err != nil {
				reply.Status = tribrpc.Exists
				return nil
			}
			reply.Status = tribrpc.OK
			return nil
		}
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.NoSuchUser
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	if ts.UserExist(args.UserID) {
		if ts.UserExist(args.TargetUserID) {
			if err := ts.libstore.RemoveFromList(util.FormatSubListKey(args.UserID), args.TargetUserID); err != nil {
				reply.Status = tribrpc.NoSuchTargetUser
				return nil
			}
			reply.Status = tribrpc.OK
			return nil
		}
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.NoSuchUser
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	if ts.UserExist(args.UserID) {
		if list, err := ts.libstore.GetList(util.FormatSubListKey(args.UserID)); err != nil {
			if list == nil {
				return err
			}
			reply.Status = tribrpc.OK
			reply.UserIDs = list
			return nil
		} else {
			reply.Status = tribrpc.OK
			reply.UserIDs = list
			return nil
		}
	}
	reply.Status = tribrpc.NoSuchUser
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	if !ts.UserExist(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
	} else {
		tribber := tribrpc.Tribble{
			UserID:   args.UserID,
			Posted:   time.Now(),
			Contents: args.Contents,
		}
		tribListKey := util.FormatTribListKey(args.UserID)
		postKey := util.FormatPostKey(args.UserID, tribber.Posted.UnixNano())

		// TribListKey -> List<PostKey>
		if err := ts.libstore.AppendToList(tribListKey, postKey); err != nil {
			return err
		}

		// PostKey -> tribble
		buf, err := json.Marshal(tribber)
		if err != nil {
			return err
		}
		if err = ts.libstore.Put(postKey, string(buf)); err != nil {
			return err
		}
		reply.PostKey = postKey
		reply.Status = tribrpc.OK
	}
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	if !ts.UserExist(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
	} else if !ts.TribbleExist(args.PostKey) {
		reply.Status = tribrpc.NoSuchPost
	} else {
		tribListKey := util.FormatTribListKey(args.UserID)
		if err := ts.libstore.RemoveFromList(tribListKey, args.PostKey); err != nil {
			return err
		}
		if err := ts.libstore.Delete(args.PostKey); err != nil {
			return err
		}
		reply.Status = tribrpc.OK
	}
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if !ts.UserExist(args.UserID) {
		reply.Status = tribrpc.NoSuchUser
	} else {
		tribListKey := util.FormatTribListKey(args.UserID)
		if postKeys, err := ts.libstore.GetList(tribListKey); err != nil {
			reply.Status = tribrpc.OK
			return nil
		} else {
			for i := len(postKeys) - 1; i >= 0; i-- {
				postKey := postKeys[i]
				if tribbleJson, err := ts.libstore.Get(postKey); err != nil {
					continue
				} else {
					var tribble tribrpc.Tribble
					if err = json.Unmarshal([]byte(tribbleJson), &tribble); err != nil {
						return err
					}
					reply.Tribbles = append(reply.Tribbles, tribble)
					if len(reply.Tribbles) >= 100 {
						break
					}
				}
			}
			reply.Status = tribrpc.OK
		}
	}
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	args_s := &tribrpc.GetSubscriptionsArgs{args.UserID}
	var reply_s tribrpc.GetSubscriptionsReply
	ts.GetSubscriptions(args_s, &reply_s)
	if reply_s.Status == tribrpc.NoSuchUser {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	UserIDs := reply_s.UserIDs
	for _, UserID := range UserIDs {
		args_t := &tribrpc.GetTribblesArgs{UserID}
		var reply_t tribrpc.GetTribblesReply
		ts.GetTribbles(args_t, &reply_t)
		reply.Tribbles = append(reply.Tribbles, reply_t.Tribbles...)
	}
	reply.Status = tribrpc.OK
	sort.Sort(ByTime(reply.Tribbles))
	if len(reply.Tribbles) > 100 {
		reply.Tribbles = reply.Tribbles[:100]
	}
	return nil
}

func (ts *tribServer) UserExist(userID string) bool {
	value, err := ts.libstore.Get(util.FormatUserKey(userID))
	return err == nil && value != ""
}

func (ts *tribServer) TribbleExist(postKey string) bool {
	_, err := ts.libstore.Get(postKey)
	return err == nil
}

type ByTime []tribrpc.Tribble

func (s ByTime) Len() int {
	return len(s)
}
func (s ByTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByTime) Less(i, j int) bool {
	return s[i].Posted.After(s[j].Posted)
}
