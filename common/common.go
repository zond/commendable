package common

const (
  Like       = "like"
  View       = "view"
  Deactivate = "deactivate"
  Recommend  = "recommend"
  Views      = "views"
  Likes      = "likes"
  Actives    = "actives"

  Reject    = "reject"
  Intersect = "intersect"
)

var Commands = []string{Like, View, Deactivate, Recommend, Views, Likes, Actives}

type Message struct {
  Type         string  `json:",omitempty"`
  User         string  `json:",omitempty"`
  Object       string  `json:",omitempty"`
  Weight       float64 `json:",omitempty"`
  DontActivate bool    `json:",omitempty"`
}

type RecommendationsRequest struct {
  Num     int
  Actives string
  Viewed  string
}
