package common

const (
  Like      = "like"
  View      = "view"
  Destroy   = "destroy"
  Recommend = "recommend"
  Views     = "views"
  Likes     = "likes"
  Actives   = "actives"
)

var Commands = []string{Like, View, Destroy, Recommend, Views, Likes, Actives}

type Message struct {
  Type   string  `json:",omitempty"`
  User   string  `json:",omitempty"`
  Object string  `json:",omitempty"`
  Weight float64 `json:",omitempty"`
}
