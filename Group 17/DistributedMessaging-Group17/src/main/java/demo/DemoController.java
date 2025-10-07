package demo;

import java.util.Scanner;

public class DemoController {

    public static void main(String[] args) {
        System.out.println("🎮 DEMO CONTROLLER - Distributed Messaging System");
        System.out.println("==================================================");

        if (args.length == 0) {
            startInteractiveMode();
        } else {
            executeCommand(args);
        }
    }

    private static void startInteractiveMode() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            printMainMenu();

            try {
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;
                int choice = Integer.parseInt(line);

                switch (choice) {
                    case 1 -> startSingleNodeDemo();
                    case 2 -> startClusterDemo();
                    case 3 -> runComprehensiveDemo();
                    case 4 -> testFaultTolerance();
                    case 5 -> testReplication();
                    case 6 -> testConsensus();
                    case 7 -> {
                        System.out.println("👋 Exiting Demo Controller");
                        return;
                    }
                    default -> System.out.println("❌ Invalid option. Please try again.");
                }
            } catch (Exception e) {
                System.out.println("❌ Error: " + e.getMessage());
            }
        }
    }

    private static void printMainMenu() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("          DEMO CONTROLLER MENU");
        System.out.println("=".repeat(50));
        System.out.println("1. 🚀 Start Single Node Demo");
        System.out.println("2. 🌐 Start 3-Node Cluster Demo (ZooKeeper)");
        System.out.println("3. 📊 Run Comprehensive Demo (prints)");
        System.out.println("4. 💥 Test Fault Tolerance (prints)");
        System.out.println("5. 🔄 Test Data Replication (prints)");
        System.out.println("6. 👑 Test Consensus Algorithm (prints)");
        System.out.println("7. ❌ Exit");
        System.out.print("Choose option: ");
    }

    private static void startSingleNodeDemo() {
        System.out.println("\n🚀 STARTING SINGLE NODE DEMO");
        System.out.println("This demonstrates basic messaging without clustering.");

        // Start a single node without ZooKeeper
        Thread demoThread = new Thread(() -> {
            MessagingNode node = new MessagingNode("node-1", 7201);
            node.start();
        });
        demoThread.setDaemon(true);
        demoThread.start();

        System.out.println("✅ Single node started on port 7201");
        System.out.println("💡 Use the node's interactive menu to send messages");
    }

    private static void startClusterDemo() {
        System.out.println("\n🌐 STARTING 3-NODE CLUSTER DEMO (ZooKeeper)");
        System.out.println("This demonstrates distributed messaging with ZooKeeper.");
        System.out.println("🔍 Expect ZooKeeper at localhost:2181");

        // Start nodes in separate threads (ZooKeeper enabled)
        startNodeInThread("node-1", 7201, true);
        startNodeInThread("node-2", 7202, true);
        startNodeInThread("node-3", 7203, true);

        System.out.println("✅ Cluster nodes started:");
        System.out.println("   - node-1: localhost:7201");
        System.out.println("   - node-2: localhost:7202");
        System.out.println("   - node-3: localhost:7203");
        System.out.println("💡 Check each node's console for leader election results");
    }

    private static void startNodeInThread(String nodeId, int port, boolean useZooKeeper) {
        Thread nodeThread = new Thread(() -> {
            try {
                Thread.sleep(800); // small stagger
                MessagingNode node = useZooKeeper
                        ? new MessagingNode(nodeId, port, "localhost:2181")
                        : new MessagingNode(nodeId, port);
                node.start();
            } catch (Exception e) {
                System.err.println("❌ Failed to start " + nodeId + ": " + e.getMessage());
            }
        });
        nodeThread.setDaemon(true);
        nodeThread.start();
    }

    private static void runComprehensiveDemo() {
        System.out.println("\n📊 RUNNING COMPREHENSIVE DEMO (printing scenarios)");
        ComprehensiveDemo demo = new ComprehensiveDemo();
        demo.runAllScenarios();
    }

    private static void testFaultTolerance() {
        System.out.println("\n💥 TESTING FAULT TOLERANCE (printing steps)");
        FaultToleranceDemo faultToleranceDemo = new FaultToleranceDemo();
        faultToleranceDemo.runTests();
    }

    private static void testReplication() {
        System.out.println("\n🔄 TESTING DATA REPLICATION (printing steps)");
        ReplicationDemo replicationDemo = new ReplicationDemo();
        replicationDemo.runTests();
    }

    private static void testConsensus() {
        System.out.println("\n👑 TESTING CONSENSUS ALGORITHM (printing steps)");
        ConsensusDemo consensusDemo = new ConsensusDemo();
        consensusDemo.runTests();
    }

    private static void executeCommand(String[] args) {
        String command = args[0].toLowerCase();

        switch (command) {
            case "single"     -> startSingleNodeDemo();
            case "cluster"    -> startClusterDemo();
            case "demo"       -> runComprehensiveDemo();
            case "fault"      -> testFaultTolerance();
            case "replicate"  -> testReplication();
            case "consensus"  -> testConsensus();
            default -> {
                System.out.println("❌ Unknown command: " + command);
                System.out.println("Valid commands: single, cluster, demo, fault, replicate, consensus");
            }
        }
    }
}

// --- Supporting demo classes (print-only; no runtime dependencies) ---
class ComprehensiveDemo {
    public void runAllScenarios() {
        System.out.println("\n🎬 COMPREHENSIVE DEMO SCENARIOS:");
        scenario1_SystemStartup();
        scenario2_MessageFlow();
        scenario3_LeaderFailure();
        scenario4_DataConsistency();
        scenario5_TimeSynchronization();
        System.out.println("\n✅ All demo scenarios completed!");
    }

    private void scenario1_SystemStartup() {
        System.out.println("\n1️⃣  SYSTEM STARTUP:");
        System.out.println("   - ZK coordination, node registration, leader election, cluster formation");
    }
    private void scenario2_MessageFlow() {
        System.out.println("\n2️⃣  MESSAGE FLOW:");
        System.out.println("   - Routed via leader, vector clocks, delivery guarantees");
    }
    private void scenario3_LeaderFailure() {
        System.out.println("\n3️⃣  LEADER FAILURE & RECOVERY:");
        System.out.println("   - Crash → re-election → redirect to new leader");
    }
    private void scenario4_DataConsistency() {
        System.out.println("\n4️⃣  DATA CONSISTENCY:");
        System.out.println("   - Quorum read/write, replication, partition behavior");
    }
    private void scenario5_TimeSynchronization() {
        System.out.println("\n5️⃣  TIME SYNCHRONIZATION:");
        System.out.println("   - HLC timestamps, ordering, skew handling");
    }
}

class FaultToleranceDemo {
    public void runTests() {
        System.out.println("\n💥 FAULT TOLERANCE TESTS:");
        System.out.println("🔍 Failure detection (heartbeats/timeouts)");
        System.out.println("🔄 Automatic failover/redirection");
        System.out.println("💾 Data recovery (replay/sync)");
        System.out.println("📉 Graceful degradation");
        System.out.println("✅ Fault tolerance tests completed!");
    }
}

class ReplicationDemo {
    public void runTests() {
        System.out.println("\n🔄 REPLICATION TESTS:");
        System.out.println("📝 Quorum writes; failure handling; durability");
        System.out.println("📖 Quorum reads; stale detection; consistency");
        System.out.println("⚖️ Consistency across nodes; conflict resolution");
        System.out.println("⚡ Latency/throughput/scalability");
        System.out.println("✅ Replication tests completed!");
    }
}

class ConsensusDemo {
    public void runTests() {
        System.out.println("\n👑 CONSENSUS TESTS:");
        System.out.println("🎯 Leader election timing and stability");
        System.out.println("🤝 Agreement/commit verification");
        System.out.println("💥 Partition and crash scenarios");
        System.out.println("📊 Overhead and performance under load");
        System.out.println("✅ Consensus tests completed!");
    }
}
