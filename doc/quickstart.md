## Kayak demo app

Kayak is lightweight library, in order to use it you'll have to supply network, storage, and external timer. The demo applications does this in the simplest way, providing in-memory storage and basic networking.

This micro-demo will lead though the basic features: adding and listing data, adding and removing processes and getting status.

First, grab the app:

```
go get github.com/stratumn/kayak/kayakdemo
```

Launch the cluster with 4 processes. The processes run in foreground, so run them in separate terminals:

```
$GOPATH/bin/kayakdemo -me 9001 -others 9001,9002,9003,9004
$GOPATH/bin/kayakdemo -me 9002 -others 9001,9002,9003,9004
$GOPATH/bin/kayakdemo -me 9003 -others 9001,9002,9003,9004
$GOPATH/bin/kayakdemo -me 9004 -others 9001,9002,9003,9004
```

Each process gets its id (specified by `-me`), and the ids of others. A process with id `9001` launches 2 services on localhost: HTTP server at TCP port `9001`, and UDP service at the same port `9001`. HTTP server is used for client-server communication, while UDP is for server-server message exchanges.

#### Quering

You can talk to the processes through the HTTP API:

```
curl http://127.0.0.1:9001/status
```

will show the status of `9001`, and

```
curl http://127.0.0.1:9004/log
```

will list the log entries of `9004`.

#### Adding new data

To append new data, POST it on `/append`:

```
curl -XPOST http://127.0.0.1:9003/append -d 1
```

will tell `9003` to propose the value "1" to the cluster. Few moments later check that the value are actually added into the logs of all processes:

```
curl http://127.0.0.1:9001/log
curl http://127.0.0.1:9002/log
curl http://127.0.0.1:9003/log
curl http://127.0.0.1:9004/log
```

#### Resiliency

To simulate resiliency, restart any process. Since the processes have no persistent storage except the information conveyed via command-line arguments, restarted process will start with no data in the log. It will then ask other peers if there are some updates, and, if yes, securely download them.

#### Membership management

New members can be added to an existing cluster. Run

```
curl -XPOST http://127.0.0.1:9002/add -d 9005
```

to add the 5th process.

Then launch the process itself, providing the ids of other peers:

```
$GOPATH/bin/kayakdemo -me 9005 -others 9001,9002,9003,9004
```

It will quickly synchnonise to have the identical log.

To remove the process `9003`, run

```
curl -XPOST http://127.0.0.1:9002/expel -d 9003
```
It doesn't matter whom to ask to add/remove a process. Once removed, the process `9003` will then stop receiving the updates, and will not be able to propose any new values.
