package main

import (
  "encoding/json"
  "flag"
  "fmt"
  "github.com/gorilla/mux"
  "github.com/zond/commendable/common"
  "github.com/zond/god/client"
  godCommon "github.com/zond/god/common"
  "github.com/zond/god/dhash"
  "github.com/zond/god/setop"
  "net"
  "net/http"
  "runtime"
  "time"
)

const (
  address        = "address"
  bufferSize     = 2048
  maxMessageSize = 8192
)

var activeObjectsKey = []byte("COMMENDABLE_ACTIVE_OBJECTS")

var ip = flag.String("ip", "127.0.0.1", "IP address to listen to.")
var port = flag.Int("port", 9191, "Port to listen to for cluster net/rpc connections. The next port will be used for the database admin HTTP service.")
var joinIp = flag.String("joinIp", "", "IP address of a node in a cluster to join.")
var joinPort = flag.Int("joinPort", 9191, "Port of a node in a cluster to join.")
var udpPort = flag.Int("udpPort", 29191, "Port to listen to for UDP/JSON recommendation data.")
var jsonPort = flag.Int("jsonPort", 29192, "Port to listen to for HTTP/JSON recommendation requests.")
var dir = flag.String("dir", address, "Where to store logfiles and snapshots. Defaults to a directory named after the listening ip/port. The empty string will turn off persistence.")
var timeout = flag.Int("activationTimeout", 14, "Number of days until views are cleared and objects are automatically destroyed.")

func viewsKey(id string) []byte {
  return []byte(fmt.Sprintf("%v_VIEWS", id))
}

func likesKey(id string) []byte {
  return []byte(fmt.Sprintf("%v_LIKES", id))
}

func handleUDP(ch chan []byte, c *client.Conn) {
  var err error
  var mess common.Message
  for bytes := range ch {
    err = json.Unmarshal(bytes, &mess)
    if err != nil {
      panic(err)
    }
    if mess.Type == common.View {
      // Create a byte encoded timestamp for now
      t := time.Now().UnixNano()
      encT := godCommon.EncodeInt64(t)
      // Make the object id active
      c.SubPut(activeObjectsKey, []byte(mess.Object), encT)
      // Create a key for the views of this user
      vKey := viewsKey(mess.User)
      // Make sure the sub tree is mirrored
      c.SubAddConfiguration(vKey, "mirrored", "yes")
      // Log this view
      c.SubPut(vKey, []byte(mess.Object), encT)
      // Create an encoded timestamp for something older than timeout
      tooOld := time.Now().Add(-time.Hour * 24 * time.Duration((*timeout))).UnixNano()
      // Delete all viewed entries with timestamp older than that
      for _, item := range c.MirrorSlice(vKey, nil, godCommon.EncodeInt64(tooOld), true, true) {
        c.SubDel(vKey, item.Value)
      }
      // Delete all active entries with timestamp older than that
      for _, item := range c.MirrorSlice(activeObjectsKey, nil, godCommon.EncodeInt64(tooOld), true, true) {
        c.SubDel(activeObjectsKey, item.Value)
      }
    } else if mess.Type == common.Like {
      // Record the liked object under user
      c.SubPut(likesKey(mess.User), []byte(mess.Object), godCommon.EncodeFloat64(mess.Weight))
      // Record the liker under the liked object
      c.SubPut(likesKey(mess.Object), []byte(mess.User), nil)
      if !mess.DontActivate {
        // Make the object id active
        c.SubPut(activeObjectsKey, []byte(mess.Object), nil)
      }
      // Create an encoded timestamp for something older than timeout
      tooOld := time.Now().Add(-time.Hour * 24 * time.Duration((*timeout))).UnixNano()
      // Delete all active entries with timestamp older than that
      for _, item := range c.MirrorSlice(activeObjectsKey, nil, godCommon.EncodeInt64(tooOld), true, true) {
        c.SubDel(activeObjectsKey, item.Value)
      }
    } else if mess.Type == common.Deactivate {
      // Remote the object id from the active objects
      c.SubDel(activeObjectsKey, []byte(mess.Object))
    }
  }
}

func receiveUDP(udpConn *net.UDPConn, ch chan []byte) {
  bytes := make([]byte, maxMessageSize)
  var read int
  var err error
  read, err = udpConn.Read(bytes)
  for err == nil {
    ch <- bytes[:read]
    read, err = udpConn.Read(bytes)
  }
}

func setupUDPService(c *client.Conn) {
  udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%v:%v", *ip, *udpPort))
  if err != nil {
    panic(err)
  }
  udpConn, err := net.ListenUDP("udp", udpAddr)
  if err != nil {
    panic(err)
  }
  ch := make(chan []byte, bufferSize)
  go receiveUDP(udpConn, ch)
  go handleUDP(ch, c)
}

func getRecommendations(w http.ResponseWriter, r *http.Request, c *client.Conn) {
  uid := mux.Vars(r)["user_id"]
  var request common.RecommendationsRequest
  if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
    panic(err)
  }
  // Create a set operation that returns the union of the likers of all objects we have liked, just returning the user key
  likersOp := &setop.SetOp{
    Merge: setop.First,
    Type:  setop.Union,
  }
  // For each object we have liked, add the likers of that flavor as a source to the union of likers
  for _, obj := range c.Slice(likesKey(uid), nil, nil, true, true) {
    likersOp.Sources = append(likersOp.Sources, setop.SetOpSource{Key: likesKey(string(obj.Key))})
  }
  // Create a set operation that returns the union of the liked objects of all likers of all objects we have liked, returning the sum of the like weights
  objectsOp := &setop.SetOp{
    Merge: setop.FloatSum,
    Type:  setop.Union,
  }
  // For each user in the union of users having liked something we like, add the objects they have liked as a source to the union of objects
  for _, user := range c.SetExpression(setop.SetExpression{
    Op: likersOp,
  }) {
    if string(user.Key) != uid {
      objectsOp.Sources = append(objectsOp.Sources, setop.SetOpSource{Key: likesKey(string(user.Key))})
    }
  }
  // Filter out whatever the user alread likes
  objectsOp = &setop.SetOp{
    Merge: setop.First,
    Type:  setop.Difference,
    Sources: []setop.SetOpSource{
      setop.SetOpSource{
        SetOp: objectsOp,
      },
      setop.SetOpSource{
        Key: likesKey(uid),
      },
    },
  }
  // If the request wanted us to do something with the objects already viewed by the user
  if request.Viewed != "" {
    var opType setop.SetOpType
    // Select the appropriate operation
    if request.Viewed == common.Intersect {
      opType = setop.Intersection
    } else if request.Viewed == common.Reject {
      opType = setop.Difference
    } else {
      panic(fmt.Errorf("%v is not a valid value for Viewed", request.Viewed))
    }
    // Create a new set operation doing the appropriate thing on the set of recommendations
    objectsOp = &setop.SetOp{
      Merge: setop.First,
      Type:  opType,
      Sources: []setop.SetOpSource{
        setop.SetOpSource{
          SetOp: objectsOp,
        },
        setop.SetOpSource{
          Key: viewsKey(uid),
        },
      },
    }
  }
  // If the request wanted us to do something with the active objects
  if request.Actives != "" {
    var opType setop.SetOpType
    // Select the appropriate operation
    if request.Actives == common.Intersect {
      opType = setop.Intersection
    } else if request.Actives == common.Reject {
      opType = setop.Difference
    } else {
      panic(fmt.Errorf("%v is not a valid value for Actives", request.Actives))
    }
    // Create a new set operation doing the appropriate thing on the set of recommendations
    objectsOp = &setop.SetOp{
      Merge: setop.First,
      Type:  opType,
      Sources: []setop.SetOpSource{
        setop.SetOpSource{
          SetOp: objectsOp,
        },
        setop.SetOpSource{
          Key: activeObjectsKey,
        },
      },
    }
  }
  // designate a sub tree to dump the liked objects in
  dumpkey := []byte(fmt.Sprintf("%v_RECOMMENDED", uid))
  // make it mirrored
  c.SubAddConfiguration(dumpkey, "mirrored", "yes")
  // make sure we clean up after ourselves
  defer c.SubClear(dumpkey)
  c.SetExpression(setop.SetExpression{
    Op:   objectsOp,
    Dest: dumpkey,
  })
  var result []common.Message
  for _, item := range c.MirrorReverseSliceLen(dumpkey, nil, true, request.Num) {
    result = append(result, common.Message{
      Object: string(item.Value),
      Weight: godCommon.MustDecodeFloat64(item.Key),
    })
  }
  w.Header().Set("Content-Type", "application/json; charset=UTF-8")
  if err := json.NewEncoder(w).Encode(result); err != nil {
    panic(err)
  }
}

func getLikes(w http.ResponseWriter, r *http.Request, c *client.Conn) {
  uid := mux.Vars(r)["user_id"]
  lKey := likesKey(uid)
  var result []common.Message
  for _, item := range c.Slice(lKey, nil, nil, true, true) {
    result = append(result, common.Message{
      Type:   common.View,
      User:   uid,
      Object: string(item.Key),
      Weight: godCommon.MustDecodeFloat64(item.Value),
    })
  }
  w.Header().Set("Content-Type", "application/json; charset=UTF-8")
  if err := json.NewEncoder(w).Encode(result); err != nil {
    panic(err)
  }
}

func getViews(w http.ResponseWriter, r *http.Request, c *client.Conn) {
  uid := mux.Vars(r)["user_id"]
  vKey := viewsKey(uid)
  var result []common.Message
  for _, item := range c.Slice(vKey, nil, nil, true, true) {
    result = append(result, common.Message{
      Type:   common.View,
      User:   uid,
      Object: string(item.Key),
    })
  }
  w.Header().Set("Content-Type", "application/json; charset=UTF-8")
  if err := json.NewEncoder(w).Encode(result); err != nil {
    panic(err)
  }
}

func getActives(w http.ResponseWriter, r *http.Request, c *client.Conn) {
  var result []common.Message
  for _, item := range c.Slice(activeObjectsKey, nil, nil, true, true) {
    result = append(result, common.Message{
      Object: string(item.Key),
    })
  }
  w.Header().Set("Content-Type", "application/json; charset=UTF-8")
  if err := json.NewEncoder(w).Encode(result); err != nil {
    panic(err)
  }
}

func setupJSONService(c *client.Conn) {
  tcpConn, err := net.Listen("tcp", fmt.Sprintf("%v:%v", *ip, *jsonPort))
  if err != nil {
    panic(err)
  }
  router := mux.NewRouter()
  router.Methods("POST").Path(fmt.Sprintf("/%v/{user_id}", common.Recommend)).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    getRecommendations(w, r, c)
  })
  router.Methods("GET").Path(fmt.Sprintf("/%v/{user_id}", common.Views)).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    getViews(w, r, c)
  })
  router.Methods("GET").Path(fmt.Sprintf("/%v/{user_id}", common.Likes)).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    getLikes(w, r, c)
  })
  router.Methods("GET").Path(fmt.Sprintf("/%v", common.Actives)).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    getActives(w, r, c)
  })
  mux := http.NewServeMux()
  mux.Handle("/", router)
  (&http.Server{Handler: mux}).Serve(tcpConn)
}

func main() {
  runtime.GOMAXPROCS(runtime.NumCPU())
  flag.Parse()
  if *dir == address {
    *dir = fmt.Sprintf("%v:%v", *ip, *port)
  }
  s := dhash.NewNodeDir(fmt.Sprintf("%v:%v", *ip, *port), *dir)
  s.MustStart()
  if *joinIp != "" {
    s.MustJoin(fmt.Sprintf("%v:%v", *joinIp, *joinPort))
  }

  c := client.MustConn(s.GetAddr())
  c.Start()

  setupUDPService(c)
  setupJSONService(c)
}
