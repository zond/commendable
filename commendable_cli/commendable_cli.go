package main

import (
  "bytes"
  "encoding/json"
  "flag"
  "fmt"
  "github.com/zond/commendable/common"
  "io"
  "net"
  "net/http"
)

var ip = flag.String("ip", "127.0.0.1", "IP address to listen to.")
var udpPort = flag.Int("udpPort", 29191, "Port to listen to for UDP/JSON recommendation data.")
var jsonPort = flag.Int("jsonPort", 29192, "Port to listen to for HTTP/JSON recommendation requests.")
var command = flag.String("cmd", "", fmt.Sprintf("Command to execute. One of %v.", common.Commands))
var userId = flag.String("uid", "", "User id that likes or views or needs a recommendation.")
var objectId = flag.String("oid", "", "Object id that is liked or viewed or destroyed.")
var weight = flag.Float64("weight", 1, "Amount of liking being done.")
var actives = flag.String("actives", "", fmt.Sprintf("How to treat active objects. Either %v the results with them, or %v them completely.", common.Intersect, common.Reject))
var viewed = flag.String("viewed", "", fmt.Sprintf("How to treat viewed objects. Either %v the results with them, or %v them completely.", common.Intersect, common.Reject))
var num = flag.Int("num", 16, "How many recommendations to provide, at most.")

func getUDPConn() *net.UDPConn {
  udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%v:%v", *ip, *udpPort))
  if err != nil {
    panic(err)
  }
  udpConn, err := net.DialUDP("udp", nil, udpAddr)
  if err != nil {
    panic(err)
  }
  return udpConn
}

func sendUDP(o interface{}) {
  conn := getUDPConn()
  bytes, err := json.Marshal(o)
  if err != nil {
    panic(err)
  }
  sent, err := conn.Write(bytes)
  if err != nil {
    panic(err)
  }
  if sent != len(bytes) {
    panic(fmt.Errorf("Failed sending the entire packet! Sent %v bytes of %v", sent, string(bytes)))
  }
}

func decode(r io.Reader) string {
  var result interface{}
  err := json.NewDecoder(r).Decode(&result)
  if err != nil {
    panic(err)
  }
  bytes, err := json.MarshalIndent(result, "", "  ")
  if err != nil {
    panic(err)
  }
  return string(bytes)
}

func httpGet(path string) string {
  client := new(http.Client)
  resp, err := client.Get(fmt.Sprintf("http://%v:%v/%v", *ip, *jsonPort, path))
  if err != nil {
    panic(err)
  }
  return decode(resp.Body)
}

func httpPost(path string, obj interface{}) string {
  client := new(http.Client)
  body := new(bytes.Buffer)
  if err := json.NewEncoder(body).Encode(obj); err != nil {
    panic(err)
  }
  resp, err := client.Post(fmt.Sprintf("http://%v:%v/%v", *ip, *jsonPort, path), "application/json; charset=UTF-8", body)
  if err != nil {
    panic(err)
  }
  return decode(resp.Body)
}

func main() {
  flag.Parse()
  if *command == common.Like {
    if *userId == "" || *objectId == "" {
      flag.PrintDefaults()
    } else {
      sendUDP(common.Message{
        Type:   common.Like,
        User:   *userId,
        Object: *objectId,
        Weight: *weight,
      })
    }
  } else if *command == common.View {
    if *userId == "" || *objectId == "" {
      flag.PrintDefaults()
    } else {
      sendUDP(common.Message{
        Type:   common.View,
        User:   *userId,
        Object: *objectId,
      })
    }
  } else if *command == common.Deactivate {
    if *objectId == "" {
      flag.PrintDefaults()
    } else {
      sendUDP(common.Message{
        Type:   common.Deactivate,
        Object: *objectId,
      })
    }
  } else if *command == common.Recommend {
    if *userId == "" {
      flag.PrintDefaults()
    } else {
      fmt.Println(httpPost(fmt.Sprintf("%v/%v", common.Recommend, *userId), common.RecommendationsRequest{
        Num:     *num,
        Actives: *actives,
        Viewed:  *viewed,
      }))
    }
  } else if *command == common.Views {
    if *userId == "" {
      flag.PrintDefaults()
    } else {
      fmt.Println(httpGet(fmt.Sprintf("%v/%v", common.Views, *userId)))
    }
  } else if *command == common.Likes {
    if *userId == "" {
      flag.PrintDefaults()
    } else {
      fmt.Println(httpGet(fmt.Sprintf("%v/%v", common.Likes, *userId)))
    }
  } else if *command == common.Actives {
    fmt.Println(httpGet(fmt.Sprintf("%v", common.Actives)))
  } else {
    flag.PrintDefaults()
  }
}
