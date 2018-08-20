### About

To avoid verticalization of code, each type of exchange between distributed processes has its own named message bearing the data of the exchange. No general containers like "KMessage" is used. The approach helps to avoid different kinds of switch cases (by type, by enum value or flag) and their combinations. It also eliminates the questions of the kind "is the field A is used when the flag F is set?". The drawback, however is that it leads to some duplication of data fields (see [types.go](../types.go)). It requires to design many types of messages, some even without payload, to represent all possible communications between distributed processes.

This document aims to clarify message exchanges in various states of BFT algorithm: normal mode, faults, catch-up and idle. Inevitably it also highlights some key points of consensus protocol; for full description see [References](../README.md#references).

### Examples

In our examples we will assume a system with six processes:

* four server processes: `A`, `B`, `C` and `D`
* one client process `L`
* one user process `U`

User process is the one that uses Kayak to store data, it's external to the library. Technically, user and client processes are parts of a single binary and communicate through funtction calls, not through the network. They're separated purely for clarity.

We will also say that the current leader is process `B`, and, if it fails, `C` should take its place.

Each process has its local timer, which is incremented each time a message or timeout is received. To reference an event of process `C` happened at time `7`, we will simply put `C_7`.

### Normal mode

Most of the time all processes are up and running, none of them runs some mallicious Byzantine code, network conditions are good, meaning that all messages are delivered in time, and all processes are perfectly synchronized, i.e. they all have the same transaction log. We will call these conditions normal mode. In normal mode there's always some flow of entries to be added, originating from clients. It is needed to distinguish normal mode from idle mode when nothing happens.

![Normal mode messages](d1.png)

### Faulty follower

![Faulty follower messages](d2.png)

### Faulty leader

![Faulty leader messages](d3.png)

### Catch-up

![Catch-up messages](d4.png)


### Idle mode

![Idle mode messages](d5.png)
