Kayak is a Byzantine fault-tolerant ordering (consensus) library, written on Go. The design and implementation of Kayak were inspired by SMaRt-BFT and Raft.

[![GoDoc](https://godoc.org/github.com/stratumn/kayak?status.svg)](https://godoc.org/github.com/stratumn/kayak)

---

### About

Kayak is a core library for a decentralized system -- it provides ordering and synchronization facilities in the presence of Byzantine faults. The design of the library gets its inspiration from [SMaRt-BFT](https://github.com/bft-smart/library) and [coreos/raft](https://github.com/coreos/etcd/tree/master/raft).

Similar to Raft, Kayak is a lightweight library. It does not include auxiliary services, such as networking or persistent storage. To run the library, the developer has to supply an implementation of network (authenticated perfect links), storage and timer.

### Status

The library is in active, but early development stage. As a consequence, no guarantees can be given for security, stability and backward-compatibility. Use only in testing / staging environment.


### Key features

Kayak implements core consensus library features, such as:

* Byzantine fault-tolerant ordering service across live processes
* Synchronization primitives for new or delayed processes

In addition, Kayak manages cluster membership through a simple protocol. The decision to add/remove a member is taken through the consensus itself, without the need of having a trusted coordinator.

The library makes a distinction between client and server. It allows to cover use-cases of both type of systems: flat decentralized, and ranked client-server.


### Design considerations

The prime objective for the library is to be simple and minimal. Simplicity means readability, easiness to spot and fix bugs and refactor. Albeit the public methods are thread-safe, their execution is deliberately synchronized via the global lock. It brings some determinism, which buys reproducibility. The entire library should be seen as a single state machine.

In its current development state stability and security should be considered of larger importance than performance and scalability.

The tests consider only public interfaces (blackbox approach). [Zmey](https://github.com/stratumn/zmey) is largely used for this purpose. The approach makes the tests quite lengthy, but it greatly simplifies frequent internal refactoring.

Kayak aims to "log everything". The chosen policy hurts performance and causes greater code duplication, but is essential to spot bugs in a live system.

### Next steps

The major priority is to test the library in the wild, being used as a consensus engine for an existing project, such as [cockroachDB](https://github.com/cockroachdb/cockroach). See [Issues](https://github.com/stratumn/kayak/issues) for more details.


### Documentation

* [Godoc](https://godoc.org/github.com/stratumn/kayak)
* [BFT and message exchanges](doc/messages.md)

### References


* (SMaRt-BFT) Alysson Neves Bessani, João Sousa, and Eduardo E. P. Alchieri. "State machine replication for the masses with BFT-SMART." *44th Annual IEEE/IFIP International Conference on Dependable Systems and Networks, DSN 2014*, pages 355–362, 2014, [pdf](https://www.di.fc.ul.pt/~bessani/publications/dsn14-bftsmart.pdf)
* (Raft) Diego Ongaro and John Ousterhout. "In Search of an Understandable Consensus Algorithm." *USENIX ATC'14 Proceedings of the 2014 USENIX conference on USENIX Annual Technical Conference*, pages 305-320, 2014, [pdf](https://ramcloud.stanford.edu/raft.pdf)
* (BFT consensus) Christian Cachin. "Yet Another Visit to Paxos." *IBM Research Zurich, Tech. Rep. RZ 3754*, 2009, [page](http://domino.watson.ibm.com/library/cyberdig.nsf/papers/5233D5F926B64F2A8525766B00383EC9)
* [SMaRt-BFT](https://github.com/bft-smart/library) implementation by A. Bessani and J. Sousa
* [Raft](https://github.com/coreos/etcd/tree/master/raft) CoreOS implementation

### License

The code is distributed under [Apache License Version 2.0](LICENSE)
