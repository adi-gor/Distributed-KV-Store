# Distributed-KV-Store
A distributed key-value store built from scratch in Java, implementing a custom TCP wire protocol, write-ahead logging, LRU eviction, and two replication strategies: simple async leader-follower and full Raft consensus.

Why two systems?

The repo contains two independent, fully working implementations that solve durability and replication differently:
1.) Simple Store
2.) Raft-Backed Store

# Core features

Storage engine

Custom text-based wire protocol (SET, GET, DEL, SETEX, TTL, STATUS) over raw TCP, thread-per-connection.
In-memory store with O(1) LRU eviction (LinkedHashMap with access-order) and TTL expiry (active sweep + passive check-on-read).

Durability

Write-ahead log with atomic rename for crash-safe persistence.
Group-commit batching: configurable fsync interval, tuned empirically against real fsync cost rather than an arbitrary default.

Replication

Simple system: single leader, asynchronous push to connected replicas, full-snapshot bootstrap for new replicas.
Raft system: leader election with randomized timeouts, log replication with conflict-resolution/backtracking, majority-based commit, batched/asynchronous persistence of currentTerm/votedFor/log, incremental append-only log persistence.

# Benchmark highlights

All numbers from local benchmarking (50 concurrent clients, 100K ops, localhost).

<table>
  <tr>
  <th>Configuration</th>
  <th>SET Throughput</th>
  <th>SET p50 Latency</th>
  </tr>
  <tr>
  <td>No fsync batching</td>
  <td>264 ops/sec</td>
  <td>145.6 ms</td>
  </tr>
  <tr>
  <td>Group-commit (5ms batch)</td>
  <td>~3,500–5,000 ops/sec</td>
  <td>~9–13 ms</td>
  </tr>
  <tr>
  <td>Raft consensus</td>
  <td>916 ops/sec</td>
  <td>37.8 ms</td>
  </tr>
  <tr>
  <td>Real Redis</td>
  <td>157,977 ops/sec</td>
  <td>0.16 ms</td>
  </tr>
</table>

Redis's default persistence acknowledges writes before fsync completes (async, weaker durability). In this store every acknowledged write is already durable on disk.

# Running it

Simple store:

Start the server (Spring Boot)<br>
KvStoreApplication

Connect a client<br>
TestClient

Commands:

> SET name aditya<br>
> GET name

Raft cluster (3 nodes):

Terminal 1-3

Configurations

RaftNode 0 7001 8001 localhost:7002 localhost:7003<br>
RaftNode 1 7002 8002 localhost:7001 localhost:7003<br>
RaftNode 2 7003 8003 localhost:7001 localhost:7002

Client (finds the leader automatically, follows it through elections):<br>
RaftKvClient

Commands:

> SET name aditya<br>
> GET name<br>
> STATUS

# Tech stack

Java 17, Spring Boot (DI/lifecycle only (no web layer), raw TCP sockets), Maven.
