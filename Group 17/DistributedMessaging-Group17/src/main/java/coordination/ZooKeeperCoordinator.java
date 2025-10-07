package coordination;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ZooKeeper-based coordination:
 * - Creates base znodes if missing
 * - Registers this node under /nodes as EPHEMERAL
 * - Leader election under /leader using EPHEMERAL_SEQUENTIAL
 *   with "watch predecessor" pattern (less churn, fewer logs).
 */
public class ZooKeeperCoordinator implements Watcher {

    private ZooKeeper zk;
    private final String nodeId;
    private final String zkAddress;

    private volatile String currentLeader; // node-*
    private volatile boolean iAmLeader = false;

    private String myCandidatePath;        // /leader/node-00000000X
    private String myNodePath;             // /nodes/<nodeId>
    private String predecessorPath;        // path we currently watch

    // ZK paths
    private static final String ROOT_PATH     = "/messaging-system";
    private static final String NODES_PATH    = ROOT_PATH + "/nodes";
    private static final String LEADER_PATH   = ROOT_PATH + "/leader";
    private static final String MESSAGES_PATH = ROOT_PATH + "/messages";
    private static final String CONFIG_PATH   = ROOT_PATH + "/config";

    private final CountDownLatch connectedLatch = new CountDownLatch(1);
    private final CountDownLatch leaderLatch    = new CountDownLatch(1);
    private final AtomicBoolean  leaderSignaled = new AtomicBoolean(false);

    public ZooKeeperCoordinator(String nodeId, String zkAddress) {
        this.nodeId = nodeId;
        this.zkAddress = zkAddress;
    }

    // --- Watcher callback for session-level and node events ---
    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedLatch.countDown();
        }

        // We only care about predecessor deletion to re-check leadership.
        if (event.getType() == Event.EventType.NodeDeleted) {
            String path = event.getPath();
            if (path != null && path.equals(predecessorPath)) {
                // Our predecessor vanished → we may now be the leader.
                try {
                    evaluateLeadership();
                } catch (Exception e) {
                    System.err.println("❌ Error while re-evaluating leadership: " + e.getMessage());
                }
            }
        }

        if (event.getState() == Event.KeeperState.Expired) {
            // Session expired: we must recreate everything.
            System.out.println("⚠️ ZooKeeper session expired. Reconnecting and re-registering...");
            try {
                reconnectAndReinit();
            } catch (Exception e) {
                System.err.println("❌ Reconnect failed: " + e.getMessage());
            }
        }
    }

    // --- Public API (exceptions wrapped so callers don’t need try/catch) ---
    public void connect() {
        try {
            System.out.println("🐘 Connecting to ZooKeeper at: " + zkAddress);
            zk = new ZooKeeper(zkAddress, 10_000, this);
            connectedLatch.await();
            System.out.println("✅ Connected to ZooKeeper successfully");

            initializePaths();
            registerNode();
            participateInElection();
        } catch (IOException | KeeperException | InterruptedException e) {
            throw new RuntimeException("Failed to connect/init ZooKeeper", e);
        }
    }

    public void waitForLeadership() throws InterruptedException {
        leaderLatch.await();
    }

    public boolean isLeader() {
        return iAmLeader;
    }

    public String getCurrentLeader() {
        return currentLeader;
    }

    public List<String> getLiveNodes() {
        try {
            return zk.getChildren(NODES_PATH, false);
        } catch (Exception e) {
            System.err.println("❌ Failed to get live nodes: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    public void storeMessageMetadata(String messageId, String metadata) {
        if (!iAmLeader) {
            System.out.println("⚠️ Not leader, skipping metadata storage");
            return;
        }
        try {
            String messagePath = MESSAGES_PATH + "/" + messageId;
            zk.create(
                messagePath,
                metadata.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT
            );
            System.out.println("💾 Stored message metadata: " + messageId);
        } catch (KeeperException.NodeExistsException e) {
            System.out.println("📝 Message metadata already exists: " + messageId);
        } catch (Exception e) {
            System.err.println("❌ Failed to store message metadata: " + e.getMessage());
        }
    }

    public void close() {
        try {
            // best-effort: delete our candidate and node ephemeral (will go away on close anyway)
            if (zk != null) {
                try {
                    if (myCandidatePath != null) {
                        safeDelete(myCandidatePath);
                    }
                    if (myNodePath != null) {
                        safeDelete(myNodePath);
                    }
                } catch (Exception ignored) {}
                zk.close();
                System.out.println("🔌 Disconnected from ZooKeeper");
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    // --- Internal setup ---
    private void initializePaths() throws KeeperException, InterruptedException {
        createIfNotExists(ROOT_PATH,     CreateMode.PERSISTENT);
        createIfNotExists(NODES_PATH,    CreateMode.PERSISTENT);
        createIfNotExists(LEADER_PATH,   CreateMode.PERSISTENT);
        createIfNotExists(MESSAGES_PATH, CreateMode.PERSISTENT);
        createIfNotExists(CONFIG_PATH,   CreateMode.PERSISTENT);
    }

    private void createIfNotExists(String path, CreateMode mode)
            throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
                System.out.println("📁 Created ZooKeeper path: " + path);
            } catch (KeeperException.NodeExistsException ignore) {
                // already exists
            }
        }
    }

    private void registerNode() throws KeeperException, InterruptedException {
        myNodePath = NODES_PATH + "/" + nodeId;
        try {
            zk.create(
                myNodePath,
                nodeId.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL
            );
            System.out.println("📝 Registered node: " + myNodePath);
        } catch (KeeperException.NodeExistsException e) {
            // If a stale ephemeral exists (previous session not yet expired), try to clean then re-create.
            System.out.println("📝 Node already registered, attempting to replace: " + myNodePath);
            safeDelete(myNodePath);
            zk.create(
                myNodePath,
                nodeId.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL
            );
            System.out.println("📝 Re-registered node: " + myNodePath);
        }
    }

    private void participateInElection() throws KeeperException, InterruptedException {
        System.out.println("🚨 Starting leader election for: " + nodeId);

        // Create ephemeral sequential candidate
        myCandidatePath = zk.create(
            LEADER_PATH + "/node-",
            nodeId.getBytes(StandardCharsets.UTF_8),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL
        );
        System.out.println("📋 Created leader candidate: " + myCandidatePath);

        evaluateLeadership(); // sets watch on predecessor if not leader
    }

    /**
     * Check if we're the smallest node under /leader.
     * If not, set a watch on the immediate predecessor only.
     */
    private void evaluateLeadership() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(LEADER_PATH, false);
        if (children.isEmpty()) return;

        Collections.sort(children);

        String myNodeName = myCandidatePath.substring(LEADER_PATH.length() + 1); // strip "/leader/"
        int myIndex = children.indexOf(myNodeName);
        if (myIndex < 0) {
            // Our candidate vanished (session issue) — rejoin election.
            System.out.println("⚠️ Candidate missing from children; rejoining election");
            participateInElection();
            return;
        }

        if (myIndex == 0) {
            // We are the smallest → leader.
            becomeLeader();
        } else {
            // Not leader; watch predecessor.
            String predecessor = children.get(myIndex - 1);
            String predecessorFullPath = LEADER_PATH + "/" + predecessor;

            // Set a watch on predecessor. When it disappears, ZK will call process() with NodeDeleted.
            Stat stat = zk.exists(predecessorFullPath, this);
            predecessorPath = predecessorFullPath;

            // Update current leader (smallest child)
            String leaderChild = children.get(0);
            updateLeaderFromPath(LEADER_PATH + "/" + leaderChild);
            becomeFollower();
        }
    }

    private void becomeLeader() throws KeeperException, InterruptedException {
        iAmLeader = true;
        currentLeader = nodeId;
        predecessorPath = null;

        System.out.println("👑 [" + nodeId + "] I am the NEW LEADER!");

        // Ensure leader node’s data contains leader’s nodeId (optional, for visibility)
        // Write our nodeId into our candidate znode (already true), nothing else required.

        if (leaderSignaled.compareAndSet(false, true)) {
            leaderLatch.countDown();
        }
    }

    private void becomeFollower() {
        if (iAmLeader) {
            System.out.println("⬇️  [" + nodeId + "] Lost leadership, now FOLLOWER");
        }
        iAmLeader = false;

        if (leaderSignaled.compareAndSet(false, true)) {
            // First time we learn leader info, allow the app to continue.
            leaderLatch.countDown();
        }

        System.out.println("✅ [" + nodeId + "] Leader is: " + currentLeader);
    }

    private void updateLeaderFromPath(String leaderPath) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(leaderPath, false);
        if (stat != null) {
            byte[] data = zk.getData(leaderPath, false, stat);
            currentLeader = new String(data, StandardCharsets.UTF_8);
        } else {
            currentLeader = null;
        }
    }

    private void reconnectAndReinit() throws Exception {
        // Close old (will be already invalid), open new, then re-init.
        try { if (zk != null) zk.close(); } catch (InterruptedException ignored) {}

        ZooKeeper newZk = new ZooKeeper(zkAddress, 10_000, this);
        this.zk = newZk;

        // Wait until connected
        // (We create a fresh latch so we don't block forever on an already-counted latch)
        CountDownLatch latch = new CountDownLatch(1);
        ZooKeeperCoordinator self = this;
        newZk.register(event -> {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                latch.countDown();
            }
            self.process(event); // delegate to normal processing too
        });
        latch.await();

        initializePaths();
        registerNode();
        participateInElection();
    }

    private void safeDelete(String path) {
        try {
            zk.delete(path, -1);
        } catch (KeeperException.NoNodeException ignore) {
        } catch (Exception e) {
            // best effort
        }
    }
}
