package demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class SystemDemo {

    public static void main(String[] args) {
        System.out.println("🎬 Distributed Messaging System - Group 17");
        System.out.println("===========================================");
        System.out.println("          SYSTEM DEMONSTRATION");
        System.out.println("===========================================");

        if (args.length == 0) {
            startInteractiveDemo();
        } else {
            executeQuickDemo(args);
        }
    }

    private static void startInteractiveDemo() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            printDemoMenu();

            try {
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;
                int choice = Integer.parseInt(line);

                switch (choice) {
                    case 1 -> demoStandaloneMode();
                    case 2 -> demoClusterMode();
                    case 3 -> demoWithZooKeeper();
                    case 4 -> runAutomatedTests();
                    case 5 -> showSystemArchitecture();
                    case 6 -> {
                        System.out.println("👋 Thank you for using the Distributed Messaging System!");
                        return;
                    }
                    default -> System.out.println("❌ Invalid option. Please try again.");
                }
            } catch (Exception e) {
                System.out.println("❌ Error: " + e.getMessage());
            }
        }
    }

    private static void printDemoMenu() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("           SYSTEM DEMO MENU");
        System.out.println("=".repeat(50));
        System.out.println("1. 🚀 Demo Standalone Mode");
        System.out.println("2. 🌐 Demo 3-Node Cluster (manual peers)");
        System.out.println("3. 🐘 Demo with ZooKeeper");
        System.out.println("4. 🧪 Run Automated Tests (print)");
        System.out.println("5. 🏗️ Show System Architecture (print)");
        System.out.println("6. ❌ Exit Demo");
        System.out.print("Choose demo type: ");
    }

    private static void demoStandaloneMode() {
        System.out.println("\n🚀 DEMONSTRATING STANDALONE MODE");
        System.out.println("This shows basic messaging without distributed coordination.");

        System.out.println("\n🎯 Starting standalone node...");
        Thread standaloneThread = new Thread(() -> {
            MessagingNode node = new MessagingNode("node-1", 7201);
            node.start();
        });
        standaloneThread.setDaemon(true);
        standaloneThread.start();

        System.out.println("✅ Standalone node started on port 7201");
        System.out.println("💡 Use the node's menu to test basic messaging");
    }

    private static void demoClusterMode() {
        System.out.println("\n🌐 DEMONSTRATING 3-NODE CLUSTER (manual peers, no ZK)");

        // Start cluster nodes without ZooKeeper, with manual peers
        startClusterNode("node-1", 7201, false);
        startClusterNode("node-2", 7202, false);
        startClusterNode("node-3", 7203, false);

        System.out.println("✅ Cluster nodes started:");
        System.out.println("   - node-1: localhost:7201");
        System.out.println("   - node-2: localhost:7202");
        System.out.println("   - node-3: localhost:7203");
    }

    private static void demoWithZooKeeper() {
        System.out.println("\n🐘 DEMONSTRATING ZOOKEEPER COORDINATION");
        System.out.println("🔍 ZooKeeper expected at localhost:2181");

        // Start nodes with ZooKeeper
        startClusterNode("node-1", 7201, true);
        startClusterNode("node-2", 7202, true);
        startClusterNode("node-3", 7203, true);

        System.out.println("✅ ZooKeeper-coordinated cluster started:");
        System.out.println("   - node-1: localhost:7201");
        System.out.println("   - node-2: localhost:7202");
        System.out.println("   - node-3: localhost:7203");
        System.out.println("💡 Watch for automatic leader election in node consoles");
    }

    private static void startClusterNode(String nodeId, int port, boolean useZooKeeper) {
        Thread nodeThread = new Thread(() -> {
            try {
                Thread.sleep(1200); // slight stagger
                MessagingNode node;
                if (useZooKeeper) {
                    node = new MessagingNode(nodeId, port, "localhost:2181");
                } else {
                    node = new MessagingNode(nodeId, port);
                    // manual peers
                    List<String> peers = new ArrayList<>();
                    if (!nodeId.equals("node-1")) peers.add("localhost:7201");
                    if (!nodeId.equals("node-2")) peers.add("localhost:7202");
                    if (!nodeId.equals("node-3")) peers.add("localhost:7203");
                    node.setPeers(peers);
                }
                node.start();
            } catch (Exception e) {
                System.err.println("❌ Failed to start " + nodeId + ": " + e.getMessage());
            }
        });
        nodeThread.setDaemon(true);
        nodeThread.start();
    }

    // — print-only helpers below —
    private static void runAutomatedTests() {
        System.out.println("\n🧪 RUNNING AUTOMATED TESTS (print-only)");
        System.out.println("🔬 Unit: Message, VectorClock, QuorumReplication, TimeSync, FailureDetector");
        System.out.println("🔗 Integration: startup, routing, election, consistency, recovery");
        System.out.println("⚡ Performance: throughput/latency/election time");
        System.out.println("💥 Fault Tolerance: node crash, leader loss, partition");
        System.out.println("✅ All automated tests completed (demo text).");
    }

    private static void showSystemArchitecture() {
        System.out.println("\n🏗️ SYSTEM ARCHITECTURE (overview)");
        System.out.println("MessagingNode ⟶ { ZooKeeperCoordinator, SimpleServer, FaultToleranceManager,");
        System.out.println("                  QuorumReplicationManager, HybridTimeSynchronizer, MessageSequencer }");
        System.out.println("\nData flow: client → any node → leader → replicate to followers → acks → client");
    }

    private static void executeQuickDemo(String[] args) {
        String demoType = args[0].toLowerCase();

        switch (demoType) {
            case "standalone"  -> demoStandaloneMode();
            case "cluster"     -> demoClusterMode();
            case "zookeeper"   -> demoWithZooKeeper();
            case "test"        -> runAutomatedTests();
            case "architecture"-> showSystemArchitecture();
            default -> {
                System.out.println("❌ Unknown demo type: " + demoType);
                System.out.println("Valid types: standalone, cluster, zookeeper, test, architecture");
            }
        }
    }
}
