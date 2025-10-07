# Distributed Messaging System - Group 17

A fault-tolerant distributed messaging system using ZooKeeper for coordination.

## ??? Architecture
- **3-Node Cluster** with ZooKeeper coordination
- **Leader-based** message routing
- **Quorum replication** for data consistency
- **Vector clocks** for causal ordering
- **Automatic failover** and recovery

## ?? Team Member Responsibilities

### Member 1: Fault Tolerance
- Message replication across nodes
- Failure detection and automatic failover
- Node recovery mechanisms

### Member 2: Data Replication & Consistency  
- Quorum-based read/write operations
- Deduplication service
- Consistency guarantees

### Member 3: Time Synchronization
- Hybrid logical-physical clocks
- Message sequencing and reordering
- Clock skew detection

### Member 4: Consensus & Agreement
- ZooKeeper-based leader election
- Distributed coordination
- Cluster membership management

## ?? Quick Start

### 1. Start ZooKeeper
```bash
scripts/start-zk.bat