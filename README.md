# 🐍 Python Redis From Scratch

This project is a **high-fidelity implementation of a Redis server** built entirely from Python.  
It features a **multi-threaded, non-blocking architecture** that strictly adheres to the **Redis Serialization Protocol (RESP)**.

The server can handle **multiple clients simultaneously** and supports a wide range of Redis data structures and system commands — providing an in-depth exploration of **database design, concurrency**, and **network programming**.

---

## ✨ Implemented Features & Commands

| **Feature** | **Commands Implemented** | **Implementation Notes** |
|--------------|---------------------------|----------------------------|
| **Basic Operations** | `PING`, `ECHO`, `GET`, `SET`, `KEYS` | Supports `PX` and `EX` arguments for key expiration. Uses *lazy expiration* for efficiency. |
| **List & Blocking** | `LPUSH`, `RPUSH`, `LPOP`, `LLEN`, `LRANGE`, `BLPOP` | Implements blocking clients with `BLPOP` using `threading.Condition` for timed waits. |
| **Sorted Sets** | `ZADD`, `ZRANGE`, `ZRANK`, `ZCARD`, `ZSCORE`, `ZREM` | Members sorted first by score, then lexicographically — preserving Redis’s ordering guarantees. |
| **Geo-Spatial** | `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH` | Spatial indexing with **Morton Geohashing** and distance calculation using the **Haversine formula**. |
| **Streams** | `XADD`, `XRANGE`, `XREAD` | Supports `*` and `ms-*` auto ID generation. `XREAD BLOCK` implemented to wake blocked clients when new data arrives. |
| **Pub/Sub** | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` | Maintains subscription lists and broadcasts messages to all listening sockets. |
| **Transactions** | `MULTI`, `EXEC`, `DISCARD` | Commands are queued between `MULTI` and `EXEC`, forming a mini state machine per client. |
| **Replication** | `INFO replication`, `REPLCONF`, `PSYNC`, `WAIT` | Implements master–replica handshake, command propagation, and durability verification with replica acknowledgements. |

---

## 🏗️ Architecture & Software Principles Learned

| **Component** | **Responsibility** | **Principles Learned** |
|----------------|--------------------|-------------------------|
| `app/main.py` | Bootstraps the server, manages sockets, and spawns a thread for each client. Handles replication handshakes. | **Concurrency**, **Multi-threading**, **Socket Programming** |
| `app/parser.py` | Parses raw TCP byte streams (RESP format) into structured Python command lists. | **Protocol Engineering**, **Byte-level Parsing** |
| `app/datastore.py` | Manages all shared data, synchronization via `threading.Lock`, and RDB persistence parsing. | **Thread Safety**, **Synchronization (Lock, Condition)**, **Persistence** |
| `app/command_execution.py` | Routes commands, executes business logic, manages transactions, Pub/Sub, and replication propagation. | **Router Design**, **State Management**, **Distributed Systems** |

---

## 🧠 Key Takeaways
- Built a **production-grade database architecture** using only core Python libraries.  
- Learned **low-level socket I/O**, **RESP protocol parsing**, and **thread synchronization**.  
- Implemented **core Redis internals** like replication, transactions, and blocking commands.  
- Gained a deep understanding of **distributed systems** and **database concurrency control**.

---

# 🔄 Redis Advanced Commands Demo

This guide demonstrates various Redis functionalities including replication, transactions, blocking list operations, geo-spatial indexing, and streams.

---

## 🔄 Replication and WAIT
> Requires Master and Replica instances running.

### **Client 1 (Master: `-p 6379`)**

```bash
# Execute a write command
SET durable_data "check"
OK

# WAIT for 1 replica to acknowledge the command, timeout after 5000ms (5 seconds)
WAIT 1 5000
(integer) 1
```

### **Client 2 (Replica: `-p 6380`)**

Verify that the command was propagated.

```bash
redis-cli -p 6380

GET durable_data
"check"

INFO replication
# replication
role:slave
master_host:localhost
master_port:6379
...
```

---

## 🔒 Transactions

```bash
redis-cli -p 6379

MULTI
OK

INCR tx_counter
QUEUED

SET tx_key "executed"
QUEUED

EXEC
1) (integer) 3   # Assuming tx_counter was 2 from before
2) OK
```

---

## 📦 List and Blocking (BLPOP)

### **Client A (Blocking):**
Start waiting on a list (`list_a`).  
This command will block the thread.

```bash
redis-cli -p 6379

BLPOP list_a 0
# The cursor will wait indefinitely...
```

### **Client B (Pusher):**
In a second terminal, push an item to the list to unblock Client A.

```bash
redis-cli -p 6379

RPUSH list_a "pushed_item"
(integer) 1
```

### **Client A's Output:**
```bash
1) "list_a"
2) "pushed_item"
```

---

## 🛰️ Geo-Spatial Indexing

```bash
redis-cli -p 6379

GEOADD cities 2.3522 48.8566 "Paris"
(integer) 1

GEOADD cities 13.4050 52.5200 "Berlin"
(integer) 1

GEODIST cities Paris Berlin km
"878.0253"

GEOSEARCH cities FROMLONLAT 8.0 50.0 BYRADIUS 500 km
1) "Paris"
2) "Berlin"
```

---

## 🌊 Streams

```bash
redis-cli -p 6379

# Add an entry, letting the server generate the full ID (*)
XADD sensor_data * temp 25 hum 60
"1729000000000-0"  # (The actual ID will be time-based)

# Read the full range of the stream
XRANGE sensor_data - +
1) 1) "1729000000000-0"
   2) 1) "temp"
      2) "25"
      3) "hum"
      4) "60"
```

---

✅ **End of Demo**

