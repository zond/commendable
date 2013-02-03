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
  "math"
  "net"
  "net/http"
  "runtime"
  "sync/atomic"
  "time"
)

const (
  address        = "address"
  bufferSize     = 1024 * 1024
  maxMessageSize = 8192
)

var activeObjectsKey = []byte("COMMENDABLE_ACTIVE_OBJECTS")
var changedUsersKey = []byte("COMMENDABLE_CHANGED_USERS")

var ip = flag.String("ip", "127.0.0.1", "IP address to listen to.")
var port = flag.Int("port", 9191, "Port to listen to for cluster net/rpc connections. The next port will be used for the database admin HTTP service.")
var joinIp = flag.String("joinIp", "", "IP address of a node in a cluster to join.")
var joinPort = flag.Int("joinPort", 9191, "Port of a node in a cluster to join.")
var udpPort = flag.Int("udpPort", 29191, "Port to listen to for UDP/JSON recommendation data.")
var jsonPort = flag.Int("jsonPort", 29192, "Port to listen to for HTTP/JSON recommendation requests.")
var dir = flag.String("dir", address, "Where to store logfiles and snapshots. Defaults to a directory named after the listening ip/port. The empty string will turn off persistence.")
var timeout = flag.Int("activationTimeout", 14, "Number of days until views are cleared and objects are automatically destroyed.")

func uViewsKey(id string) []byte {
  return []byte(fmt.Sprintf("USER_%v_VIEWS", id))
}

func uLikesKey(id string) []byte {
  return []byte(fmt.Sprintf("USER_%v_LIKES", id))
}

func oLikesKey(id string) []byte {
  return []byte(fmt.Sprintf("OBJECT_%v_LIKES", id))
}

func uRecommendedKey(id string) []byte {
  return []byte(fmt.Sprintf("USER_%v_RECOMMENDED", id))
}

func updateRecommendations(c *client.Conn, queueSize *int32) {
  var uidB []byte
  var tsB []byte
  var existed bool
  for {
    for atomic.LoadInt32(queueSize) > 0 || c.SubSize(changedUsersKey) == 0 {
      time.Sleep(time.Second)
    }
    tsB, uidB, existed = c.First(changedUsersKey)
    if existed {
      fmt.Println("updating the recommendations for", string(uidB))
      c.SubDel(changedUsersKey, tsB)
    }
  }
}

func handleUDP(ch chan []byte, c *client.Conn, queueSize *int32) {
  var err error
  var mess common.Message
  for bytes := range ch {
    err = json.Unmarshal(bytes, &mess)
    if err == nil {
      if mess.Type == common.View {
        // Create a byte encoded timestamp for now
        t := time.Now().UnixNano()
        encT := godCommon.EncodeInt64(t)
        // Make the object id active
        c.SubPut(activeObjectsKey, []byte(mess.Object), encT)
        // Create a key for the views of this user
        vKey := uViewsKey(mess.User)
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
        c.SubPut(uLikesKey(mess.User), []byte(mess.Object), godCommon.EncodeFloat64(mess.Weight))
        // Record the liker under the liked object
        c.SubPut(oLikesKey(mess.Object), []byte(mess.User), nil)
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
        c.SubPut(changedUsersKey, godCommon.EncodeInt64(time.Now().UnixNano()), []byte(mess.User))
      } else if mess.Type == common.Deactivate {
        // Remote the object id from the active objects
        c.SubDel(activeObjectsKey, []byte(mess.Object))
      }
    } else {
      fmt.Printf("When parsing %v: %v\n", string(bytes), err)
    }
    atomic.AddInt32(queueSize, -1)
  }
}

func receiveUDP(udpConn *net.UDPConn, ch chan []byte, queueSize *int32) {
  bytes := make([]byte, maxMessageSize)
  read, err := udpConn.Read(bytes)
  for err == nil {
    atomic.AddInt32(queueSize, 1)
    ch <- bytes[:read]
    bytes = make([]byte, maxMessageSize)
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
  var queueSize int32
  go receiveUDP(udpConn, ch, &queueSize)
  go handleUDP(ch, c, &queueSize)
  go updateRecommendations(c, &queueSize)
}

func refreshRecommendations(c *client.Conn, uid string) {
  rKey := uRecommendedKey(uid)
  c.SubClear(rKey)
  // Create a set operation that returns the union of the likers of all objects we have liked, just returning the user key
  likersOp := &setop.SetOp{
    Merge: setop.First,
    Type:  setop.Union,
  }
  // For each object we have liked, add the likers of that flavor as a source to the union of likers
  for _, obj := range c.Slice(uLikesKey(uid), nil, nil, true, true) {
    fmt.Printf("%v likes %v, adding its likers\n", uid, string(obj.Key))
    likersOp.Sources = append(likersOp.Sources, setop.SetOpSource{Key: oLikesKey(string(obj.Key))})
  }
  // Create a set operation that returns the union of all things liked by all likers of objects we liked, returning the sum of the likes for each object
  objectsOp := &setop.SetOp{
    Merge: setop.FloatSum,
    Type:  setop.Union,
  }
  // For each user in the union of users having liked something we like
  for _, user := range c.SetExpression(setop.SetExpression{
    Op: likersOp,
  }) {
    // If the user is not us
    if string(user.Key) != uid {
      // Fetch the number of objects we have both liked
      similarity := len(c.SetExpression(setop.SetExpression{
        Code: fmt.Sprintf("(I:First %v %v)", string(uLikesKey(string(user.Key))), string(uLikesKey(uid))),
      }))
      // And weight the user according to how many commonalities we have
      weight := math.Log(float64(similarity + 1))
      fmt.Printf("%v likes %v objects in common with %v, giving it a weight of %v\n", string(user.Key), similarity, uid, weight)
      // Add the objects liked by this user, weighed this much, as a source
      objectsOp.Sources = append(objectsOp.Sources, setop.SetOpSource{Key: uLikesKey(string(user.Key)), Weight: &weight})
    }
  }
  // Dump the results in the dump tree
  c.SetExpression(setop.SetExpression{
    Op:   objectsOp,
    Dest: rKey,
  })
}

func getRecommendations(w http.ResponseWriter, r *http.Request, c *client.Conn) {
  uid := mux.Vars(r)["user_id"]
  var request common.RecommendationsRequest
  if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
    panic(err)
  }
  // Get the key for the recommendations for this user
  rKey := uRecommendedKey(uid)
  if c.SubSize(rKey) == 0 {
    refreshRecommendations(c, uid)
  }
  // Create a dump key to store the finished recomendations in
  dumpkey := []byte(fmt.Sprintf("USER_%v_FILTERED", uid))
  // make it mirrored
  c.SubAddConfiguration(dumpkey, "mirrored", "yes")
  // Create a simple Union of the recommendations
  recOp := &setop.SetOp{
    Merge: setop.First,
    Type:  setop.Union,
    Sources: []setop.SetOpSource{
      setop.SetOpSource{
        Key: rKey,
      },
    },
  }
  // If we want to filter on active objects
  if request.Actives != "" {
    // Create an operation on the simple Union and the active objects
    recOp = &setop.SetOp{
      Merge: setop.First,
      Sources: []setop.SetOpSource{
        setop.SetOpSource{
          SetOp: recOp,
        },
        setop.SetOpSource{
          Key: activeObjectsKey,
        },
      },
    }
    // And make it of the correct type
    if request.Actives == common.Reject {
      recOp.Type = setop.Difference
    } else if request.Actives == common.Intersect {
      recOp.Type = setop.Intersection
    }
  }
  // If we want to filter on viewed objects
  if request.Viewed != "" {
    // Create an operation on the previous operation and the viewed objects
    recOp = &setop.SetOp{
      Merge: setop.First,
      Sources: []setop.SetOpSource{
        setop.SetOpSource{
          SetOp: recOp,
        },
        setop.SetOpSource{
          Key: uViewsKey(uid),
        },
      },
    }
    // And make it of the correct type
    if request.Viewed == common.Reject {
      recOp.Type = setop.Difference
    } else if request.Viewed == common.Intersect {
      recOp.Type = setop.Intersection
    }
  }
  // Run the expression
  c.SetExpression(setop.SetExpression{
    Op:   recOp,
    Dest: dumpkey,
  })
  // Finally, fetch the wanted number of recommendations
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
  lKey := uLikesKey(uid)
  var result []common.Message
  for _, item := range c.Slice(lKey, nil, nil, true, true) {
    result = append(result, common.Message{
      Type:   common.Like,
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
  vKey := uViewsKey(uid)
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
