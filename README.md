commendable
===========

A recommendation engine based on an embedded https://github.com/zond/god

It opens up a port listening for JSON encoded UDP messages containing mutating commands. 

Use this to let your app modify the recommendation state without incurring lots of extra dependencies or costs.

It also opens up a port listening serving JSON over HTTP to inspect the recommendation state, or to fetch recommendations.

# Install

Install <a href="http://golang.org/doc/install">Go</a>, gcc, git, mercurial.

Run `go get github.com/zond/commendable/commendable_server`

# Start

`$GOPATH/bin/commendable_server`

## Start options

* `-listenIp=ADDRESS`: The ip address to listen to requests at.
* `-broadcastIp=ADDRESS`: The ip address to advertise to the cluster.
* `-port=PORT`: The port to use when talking to the cluster.
* `-joinIp=ADDRESS`: The address to contact to join the cluster.
* `-joinPort=PORT`: The port to contact to join the cluster.
* `-udpPort=PORT`: The port to listen for UDP messages at.
* `-jsonPort=PORT`: The port to listen for JSON HTTP requests at.
* `-dir=DIR`: The directory to look for and store the persistence files in.
* `-timeout=DAYS`: The number of days before active objects get automatically deactivated.

# Seed your database

Send UDP messages looking like

    {
      "Type": "like",
      "User": USERID,
      "Object": OBJECTID,
      "Weight": WEIGHT,
      "DontActivate": true,
    }

for each known like in the database to the `-udpPort=PORT` port.

# Like stuff

Send UDP messages looking like

    {
      "Type": "like",
      "User": USERID,
      "Object": OBJECTID,
      "Weight": WEIGHT,
    }

to the `-udpPort=PORT` port. The object will also be considered active until `-timeout=DAYS` have passed, or it gets deactivated.

# View stuff

Send UDP messages looking like

    {
      "Type": "view",
      "User": USERID,
      "Object": OBJECTID,
    }

to the `-udpPort=PORT` port. The object will also be considered active until `-timeout=DAYS` have passed, or it gets deactivated.

# Deactivate stuff

Send UDP messages looking like

    {
      "Type": "deactivate",
      "Object": OBJECTID,
    }

to the `-udpPort=PORT` port.

# Get recommendations

`POST /recommend/USERID` messages looking like

    {
      "Num": WANTED_NUMBER_OF_RECOMMENDATIONS
      "Actives": "reject" or "intersect" or not at all,
      "Viewed": "reject" or "intersect" or not at all,
    }

to the `-jsonPort=PORT`. 

Rejecting Actives will show only non active objects. 
Intersecting Actives will show only active objects. 

Rejecting Viewed will show only objects not viewed by the user.
Intersecting Viewed will show only objects viewed by the user.

