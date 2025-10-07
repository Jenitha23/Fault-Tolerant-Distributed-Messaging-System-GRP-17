package demo;

import java.util.ArrayList;
import java.util.List;

public class NodeStarter {
    public static void main(String[] args) {
        printBanner();

        if (args.length < 2) {
            printUsage();
            return;
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);
        String zkAddress = args.length > 2 ? args[2] : null;

        validateNodeId(nodeId);
        validatePort(port);

        System.out.println("🚀 Starting " + nodeId + " on port " + port);

        MessagingNode node;
        if (zkAddress != null) {
            System.out.println("🐘 Using ZooKeeper at: " + zkAddress);
            node = new MessagingNode(nodeId, port, zkAddress);
        } else {
            System.out.println("⚠️  Running without ZooKeeper (Standalone Mode)");
            node = new MessagingNode(nodeId, port);
        }

        if (zkAddress == null) {
            setupStaticPeers(node, nodeId);
        }

        addShutdownHook(node);
        node.start();
    }

    private static void printBanner() {
        System.out.println("===========================================");
        System.out.println("   Distributed Messaging System - Group 17");
        System.out.println("           ZOOKEEPER EDITION");
        System.out.println("===========================================");
    }

    private static void printUsage() {
        System.out.println("\n📖 USAGE:");
        System.out.println("  With ZooKeeper:");
        System.out.println("    java demo.NodeStarter <node-id> <port> <zk-address>");
        System.out.println("    Example: java demo.NodeStarter node-1 7201 localhost:2181");
        System.out.println("");
        System.out.println("  Without ZooKeeper (Standalone):");
        System.out.println("    java demo.NodeStarter <node-id> <port>");
        System.out.println("    Example: java demo.NodeStarter node-1 7201");
        System.out.println("");
        System.out.println("📝 Valid node IDs: node-1, node-2, node-3, etc.");
        System.out.println("📝 Port range: 1024-65535");
    }

    private static void validateNodeId(String nodeId) {
        if (!nodeId.matches("node-[1-9][0-9]*")) {
            System.err.println("❌ Invalid node ID: " + nodeId);
            System.err.println("   Must be in format: node-1, node-2, node-3, ...");
            System.exit(1);
        }
    }

    private static void validatePort(int port) {
        if (port < 1024 || port > 65535) {
            System.err.println("❌ Invalid port: " + port);
            System.err.println("   Port must be between 1024 and 65535");
            System.exit(1);
        }
    }

    private static void setupStaticPeers(MessagingNode node, String nodeId) {
        List<String> peers = new ArrayList<>();

        switch (nodeId) {
            case "node-1" -> { peers.add("localhost:7202"); peers.add("localhost:7203"); }
            case "node-2" -> { peers.add("localhost:7201"); peers.add("localhost:7203"); }
            case "node-3" -> { peers.add("localhost:7201"); peers.add("localhost:7202"); }
            default -> { peers.add("localhost:7201"); peers.add("localhost:7202"); peers.add("localhost:7203"); }
        }

        node.setPeers(peers);
        System.out.println("📡 Static peers configured: " + peers);
    }

    private static void addShutdownHook(MessagingNode node) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🛑 Received shutdown signal");
            node.stop();
        }));
    }
}
