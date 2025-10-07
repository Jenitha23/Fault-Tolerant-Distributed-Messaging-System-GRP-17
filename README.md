# Fault-Tolerant-Distributed-Messaging-System-GRP-17
Distributed Systems Group Assignment
| Name                 | Student ID | Email                                                   |
| -------------------- | ---------- | ------------------------------------------------------- |
| Balasegaram Suthagar | IT23184930 | [it23184930@my.sliit.lk](mailto:it23184930@my.sliit.lk) |
| Jenitha Johnson Maxi | IT23596566 | [it23596566@my.sliit.lk](mailto:it23596566@my.sliit.lk) |
| Thayaparan Sanojan   | IT23619180 | [it23619180@my.sliit.lk](mailto:it23619180@my.sliit.lk) |
| A V G R Tharumina    | IT23369924 | [it23369924@my.sliit.lk](mailto:it23369924@my.sliit.lk) |

## ⚙️ How to Run the System

Follow the steps below to run the Distributed Messaging System:

### 🧩 Steps Overview
1. **Start ZooKeeper**  
2. **Compile Java sources** into `bin/` with external JARs in `lib/`  
3. **Start individual nodes** via `demo.NodeStarter`  
4. **Run the interactive menu** using `demo.DemoController`  
5. **(Optional)** Pass non-interactive arguments directly to `demo.DemoController`

---

### 🚀 Commands

#### 1️⃣ Start ZooKeeper
```bash
zkServer.cmd
2️⃣ Compile Java Sources

javac -d bin -cp "lib/*" src/**/*.java
3️⃣ Start Individual Nodes

java -cp "bin;lib/*" demo.NodeStarter <nodeId> <port> <peerPorts>
