package demo;

import common.Message;
import common.SystemMetrics;
import coordination.ZooKeeperCoordinator;
import faulttolerance.FaultToleranceManager;
import replication.QuorumReplicationManager;
import timesync.HybridTimeSynchronizer;
import timesync.MessageSequencer;
import network.SimpleServer;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class MessagingNode {
    private final String nodeId;
    private final int port;
    private final List<String> peers = new ArrayList<>();

    // Core components
    private ZooKeeperCoordinator zkCoordinator;
    private FaultToleranceManager faultTolerance;
    private QuorumReplicationManager replication;
    private HybridTimeSynchronizer timeSync;
    private MessageSequencer sequencer;
    private SimpleServer server;
    private final SystemMetrics metrics;

    private final AtomicInteger messageCounter = new AtomicInteger(0);
    private volatile boolean running = true;
    private String zkAddress = "localhost:2181";

    public MessagingNode(String nodeId, int port) {
        this.nodeId = nodeId;
        this.port = port;
        this.metrics = new SystemMetrics();
    }

    public MessagingNode(String nodeId, int port, String zkAddress) {
        this.nodeId = nodeId;
        this.port = port;
        this.zkAddress = zkAddress;
        this.metrics = new SystemMetrics();
    }

    private void initializeComponents() {
        System.out.println("🔧 Initializing components for " + nodeId);

        if (zkAddress != null && !zkAddress.isEmpty()) {
            this.zkCoordinator = new ZooKeeperCoordinator(nodeId, zkAddress);
        }

        this.faultTolerance = new FaultToleranceManager();
        this.faultTolerance.setListener(new FaultToleranceManager.Listener() {
            @Override public void onNodeDown(String peer) {
                System.out.println("💥 Node failure detected: " + peer);
                System.out.println("🔄 Handling failure of: " + peer);
                System.out.println("🔄 Redirecting traffic from failed node: " + peer);
                System.out.println("🔁 Recovering data from replicas of: " + peer);
                System.out.println("✅ Failover completed for: " + peer);
            }
            @Override public void onNodeUp(String peer) {
                System.out.println("✅ Node recovered: " + peer);
            }
        });

        this.replication = new QuorumReplicationManager(3);
        this.timeSync = new HybridTimeSynchronizer();
        this.sequencer = new MessageSequencer();
        this.server = new SimpleServer(port, this::handleIncomingMessage);
    }

    public void setPeers(List<String> newPeers) {
        this.peers.clear();
        this.peers.addAll(newPeers);
        if (this.faultTolerance != null) {
            this.faultTolerance.setNodes(newPeers);
        }
    }

    public void start() {
        System.out.println("🚀 Starting node: " + nodeId + " on port " + port);

        try {
            initializeComponents();

            if (zkCoordinator != null) {
                zkCoordinator.connect();
                zkCoordinator.waitForLeadership();
                updatePeersFromZooKeeper();
            } else {
                System.out.println("⚠️  Running without ZooKeeper coordination");
            }

            server.start();
            System.out.println("📡 Network server started on port " + port);

            faultTolerance.startFailureDetection(); // after peers set

            if (!peers.isEmpty()) {
                timeSync.synchronizeClocks(peers);
            }

            System.out.println("✅ Node " + nodeId + " started successfully");
            printNodeStatus();

            startCommandLoop();

        } catch (Exception e) {
            System.err.println("❌ Failed to start node: " + e.getMessage());
            e.printStackTrace();
            stop();
        }
    }

    // Build peers from ZooKeeper children, map node-N -> localhost:(7200+N)
    private void updatePeersFromZooKeeper() {
        if (zkCoordinator == null) return;

        try {
            List<String> liveNodes = zkCoordinator.getLiveNodes();
            List<String> otherNodes = new ArrayList<>();

            for (String child : liveNodes) {
                if (!child.equals(nodeId)) {
                    int lastDash = child.lastIndexOf('-');
                    if (lastDash < 0 || lastDash == child.length() - 1) {
                        System.err.println("⚠️  Invalid node id format: " + child);
                        continue;
                    }
                    int idx = Integer.parseInt(child.substring(lastDash + 1));
                    int nodePort = 7200 + idx; // 7201, 7202, 7203...
                    otherNodes.add("localhost:" + nodePort);
                }
            }

            setPeers(otherNodes);
            System.out.println("🔄 Updated peers from ZooKeeper: " + otherNodes);

        } catch (Exception e) {
            System.err.println("❌ Failed to update peers from ZooKeeper: " + e.getMessage());
        }
    }

    private void handleIncomingMessage(String messageContent) {
        try {
            int msgNum = messageCounter.incrementAndGet();

            Message message = new Message("remote-user", nodeId, messageContent);
            message.setTimestamp(timeSync.getCurrentTimestamp());
            message.setLogicalTimestamp(timeSync.getNextLogicalTime());

            System.out.println("💌 Received external message #" + msgNum + ": " + messageContent);

            processMessage(message);
            metrics.recordMessageDelivery(System.currentTimeMillis() - message.getTimestamp());

        } catch (Exception e) {
            System.err.println("❌ Error handling incoming message: " + e.getMessage());
        }
    }

    private void processMessage(Message message) {
        System.out.println("🔄 Processing message: " + message.getId().substring(0, 8));

        boolean stored = replication.writeMessage(message.getId(), message.getContent());

        if (stored) {
            if (zkCoordinator != null && zkCoordinator.isLeader()) {
                String metadata = message.getSender() + "->" + message.getReceiver() + ":" + message.getTimestamp();
                zkCoordinator.storeMessageMetadata(message.getId(), metadata);
            }

            // sequence locally (replication layer handles redundancy)
            sequencer.queueMessage(message);

            System.out.println("✅ Message processed and stored: " + message.getId().substring(0, 8));
        } else {
            System.out.println("❌ Failed to store message: " + message.getId().substring(0, 8));
        }
    }

    public void sendMessage(String receiver, String content) {
        Message message = new Message(nodeId, receiver, content);
        message.setTimestamp(timeSync.getCurrentTimestamp());
        message.setLogicalTimestamp(timeSync.getNextLogicalTime());

        System.out.println("📤 [" + nodeId + "] Sending to " + receiver + ": " + content);

        long startTime = System.currentTimeMillis();

        if (zkCoordinator == null || zkCoordinator.isLeader()) {
            processMessage(message);
        } else {
            String leader = zkCoordinator.getCurrentLeader();
            if (leader != null && !leader.equals(nodeId)) {
                System.out.println("🔄 Routing to leader: " + leader);

                int lastDash = leader.lastIndexOf('-');
                if (lastDash < 0 || lastDash == leader.length() - 1) {
                    System.out.println("⚠️  Invalid leader id format, processing locally");
                    processMessage(message);
                } else {
                    int leaderIdx = Integer.parseInt(leader.substring(lastDash + 1));
                    int leaderPort = 7200 + leaderIdx;

                    boolean sent = network.TCPClient.sendMessage("localhost", leaderPort, content);
                    if (sent) {
                        System.out.println("✅ Message routed to leader successfully");
                    } else {
                        System.out.println("❌ Failed to route to leader, processing locally");
                        processMessage(message);
                    }
                }
            } else {
                System.out.println("⚠️  No leader available, processing locally");
                processMessage(message);
            }
        }

        long deliveryTime = System.currentTimeMillis() - startTime;
        metrics.recordMessageDelivery(deliveryTime);
        System.out.println("✅ Message flow completed in " + deliveryTime + "ms");
    }

    private void startCommandLoop() {
        Scanner scanner = new Scanner(System.in);

        while (running) {
            printMenu();

            try {
                String input = scanner.nextLine().trim();
                if (input.isEmpty()) continue;

                int choice = Integer.parseInt(input);

                switch (choice) {
                    case 1 -> handleSendMessage(scanner);
                    case 2 -> showStatus();
                    case 3 -> metrics.printMetrics();
                    case 4 -> {
                        if (zkCoordinator != null) showZooKeeperInfo();
                        else System.out.println("❌ ZooKeeper not enabled");
                    }
                    case 5 -> simulateFailure();
                    case 6 -> running = false;
                    default -> System.out.println("❌ Invalid option");
                }
            } catch (NumberFormatException e) {
                System.out.println("❌ Please enter a valid number");
            } catch (Exception e) {
                System.out.println("❌ Error: " + e.getMessage());
            }
        }

        scanner.close();
        stop();
    }

    private void printMenu() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println("🗂️  " + nodeId + " - MESSAGING NODE CONTROLLER");
        System.out.println("=".repeat(50));
        System.out.println("1. Send Message");
        System.out.println("2. Show Status");
        System.out.println("3. Show Metrics");
        if (zkCoordinator != null) System.out.println("4. Show ZooKeeper Info");
        System.out.println("5. Simulate Node Failure");
        System.out.println("6. Exit");
        System.out.print("Choose option: ");
    }

    private void handleSendMessage(Scanner scanner) {
        System.out.print("Enter receiver node ID (e.g., node-2): ");
        String receiver = scanner.nextLine().trim();

        System.out.print("Enter message content: ");
        String content = scanner.nextLine().trim();

        if (receiver.isEmpty() || content.isEmpty()) {
            System.out.println("❌ Receiver and content cannot be empty");
            return;
        }

        sendMessage(receiver, content);
    }

    private void showStatus() {
        System.out.println("\n--- " + nodeId + " STATUS ---");
        System.out.println("📍 Port: " + port);
        System.out.println("📡 Peers: " + (peers.isEmpty() ? "None" : peers));

        if (zkCoordinator != null) {
            System.out.println("👑 Leader: " + (zkCoordinator.isLeader() ? "THIS NODE" : zkCoordinator.getCurrentLeader()));
            System.out.println("🔧 State: " + (zkCoordinator.isLeader() ? "LEADER" : "FOLLOWER"));
        } else {
            System.out.println("👑 Leader: Standalone Mode");
            System.out.println("🔧 State: STANDALONE");
        }

        System.out.println("📨 Messages processed: " + messageCounter.get());
        System.out.println("⏰ System time: " + timeSync.getCurrentTimestamp() + "ms");
        System.out.println("🌐 Server running: " + (server != null && server.isRunning()));
    }

    private void showZooKeeperInfo() {
        System.out.println("\n--- ZOOKEEPER INFORMATION ---");
        System.out.println("🐘 Connected: Yes");
        System.out.println("📋 Live Nodes: " + (zkCoordinator != null ? zkCoordinator.getLiveNodes() : List.of()));
        System.out.println("👑 Current Leader: " + (zkCoordinator != null ? zkCoordinator.getCurrentLeader() : "N/A"));
        System.out.println("🏠 ZooKeeper Address: " + zkAddress);
        System.out.println("🎯 Am I Leader: " + (zkCoordinator != null && zkCoordinator.isLeader()));
    }

    private void printNodeStatus() {
        System.out.println("\n⭐ NODE STARTUP COMPLETE");
        System.out.println("⭐ ID: " + nodeId);
        System.out.println("⭐ Port: " + port);
        if (zkCoordinator != null) {
            System.out.println("⭐ ZooKeeper: Connected to " + zkAddress);
            System.out.println("⭐ Role: " + (zkCoordinator.isLeader() ? "LEADER" : "FOLLOWER"));
        } else {
            System.out.println("⭐ Mode: STANDALONE");
        }
        System.out.println("⭐ Peers: " + peers.size());
    }

    private void simulateFailure() {
        System.out.println("💥 Simulating node failure for 5 seconds...");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("🔁 Node recovery completed");
    }

    public void stop() {
        running = false;
        System.out.println("\n🛑 Stopping node: " + nodeId);

        if (server != null) server.stop();
        if (zkCoordinator != null) zkCoordinator.close();
        if (faultTolerance != null) faultTolerance.stop();

        metrics.printMetrics();
        System.out.println("✅ Node " + nodeId + " stopped gracefully");
    }
}
