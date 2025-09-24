# lite-raft
Build a distributed consistency application based on the etcd/raft library and with reference to etcd/raft/example. 

Specifically, implement linearizable reads via the ReadIndex protocol, build a state machine application by developing an in-memory state machine interface, introduce a timeout/return-until-commit mechanism for client proposal requests, and use the Pebble database instead of Write-Ahead Log (WAL) to store Raft logs and snapshots.

## See Also

- [etcd](https://github.com/etcd-io/raft)
- [raft paper Chinese translation](https://github.com/OneSizeFitsQuorum/raft-thesis-zh_cn)