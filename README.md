commendable
===========

A recommendation engine.

It opens up a port listening for JSON encoded UDP messages containing mutating commands. 

Use this to let your app modify the recommendation state without incurring lots of extra dependencies or costs.

It also opens up a port listening serving JSON over HTTP to inspect the recommendation state, or to fetch recommendations.

# Install

Install <a href="http://golang.org/doc/install">Go</a>, gcc, git, mercurial.

Run `go get github.com/zond/commendable/commendable_server`

# Seed your database

Send UDP messages looking like

    {
      "Type": "like",
      "User": USERID,
      "Object": OBJECTID,
      "Weight": WEIGHT,
      "DontActivate": true,
    }

for each known like in the database.

    
