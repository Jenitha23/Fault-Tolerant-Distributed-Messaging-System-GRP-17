package coordination;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Backup leader election (when ZooKeeper isn't available).
 * Implements a Bully-style approach: the node with the highest numeric rank wins.
 *
 * Supported identifiers:
 *  - "node-2", "zk-node-3"  -> rank = trailing number after last '-'
 *  - "localhost:7202"       -> rank = port - 7200 (so 7202 -> 2)
 *  - Any other string       -> rank = 0 (lowest)
 */
public class LeaderElector {

    /**
     * Elect a leader from the list (highest rank wins).
     * Returns null if list is null/empty.
     */
    public static String electLeader(List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            System.out.println("⚠️  electLeader called with empty node list");
            return null;
        }

        List<String> copy = new ArrayList<>(nodes);
        copy.removeIf(Objects::isNull);

        if (copy.isEmpty()) {
            System.out.println("⚠️  electLeader: all nodes were null/invalid");
            return null;
        }

        System.out.println("🚨 Starting bully election with nodes: " + copy);

        // Highest rank first; tie-break by lexicographic order for stability
        copy.sort(Comparator
                .comparingInt(LeaderElector::rankOf).reversed()
                .thenComparing(Comparator.naturalOrder()));

        String leader = copy.get(0);
        System.out.println("👑 Elected leader via bully algorithm: " + leader + " (rank=" + rankOf(leader) + ")");
        return leader;
        }

    /**
     * Check if the given nodeId is eligible to be leader (i.e., has the highest rank).
     * If liveNodes is empty, return true (single node is leader).
     */
    public static boolean isEligibleForLeadership(String nodeId, List<String> liveNodes) {
        if (nodeId == null) return false;
        if (liveNodes == null || liveNodes.isEmpty()) return true;

        int myRank = rankOf(nodeId);
        int highestRank = liveNodes.stream()
                .filter(Objects::nonNull)
                .mapToInt(LeaderElector::rankOf)
                .max()
                .orElse(Integer.MIN_VALUE);

        if (myRank > highestRank) return true;
        if (myRank < highestRank) return false;

        // Tie-breaker: lexicographically highest wins (stable + deterministic)
        String highestLex = liveNodes.stream()
                .filter(Objects::nonNull)
                .filter(n -> rankOf(n) == highestRank)
                .max(String::compareTo)
                .orElse(null);

        return nodeId.equals(highestLex);
    }

    /**
     * Simulate a bully election with a "wait for higher nodes" phase.
     * We "wait" for a bit; if no higher-ranked node "responds", currentNode becomes leader.
     * This is only a fallback demo (real system should rely on ZooKeeper).
     */
    public static String simulateElectionWithTimeouts(String currentNode, List<String> nodes, long timeoutMs) {
        if (currentNode == null) return null;
        if (nodes == null || nodes.isEmpty()) return currentNode;

        System.out.println("⏰ Starting election with timeout: " + timeoutMs + "ms (me=" + currentNode + ", rank=" + rankOf(currentNode) + ")");

        int myRank = rankOf(currentNode);

        List<String> higherNodes = nodes.stream()
                .filter(Objects::nonNull)
                .filter(n -> rankOf(n) > myRank)
                .sorted(Comparator.comparingInt(LeaderElector::rankOf).reversed().thenComparing(Comparator.naturalOrder()))
                .collect(Collectors.toList());

        if (higherNodes.isEmpty()) {
            System.out.println("✅ No higher nodes found, I am the leader: " + currentNode);
            return currentNode;
        }

        System.out.println("📨 Waiting for responses from higher nodes: " + higherNodes);

        try {
            // Simulate half-time waiting for responses
            Thread.sleep(Math.max(0, timeoutMs / 2));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Optional: simulate a random "response" from higher nodes (to mimic contention)
        boolean someoneResponded = ThreadLocalRandom.current().nextInt(100) < 10; // 10% chance
        if (someoneResponded) {
            String winner = electLeader(higherNodes);
            System.out.println("📣 Higher node responded, they become leader: " + winner);
            return winner;
        }

        System.out.println("👑 No response from higher nodes, becoming leader: " + currentNode);
        return currentNode;
    }

    // -----------------------
    // Internal helpers
    // -----------------------

    /**
     * Compute a numeric rank for a node identifier.
     * Rules:
     *  - "node-7", "zk-node-7" -> 7  (take the number after last '-')
     *  - "localhost:7203"      -> 3  (port - 7200; supports your fixed 7201.. scheme)
     *  - anything else         -> 0
     */
    private static int rankOf(String node) {
        if (node == null || node.isEmpty()) return 0;

        // Pattern: host:port (derive from port)
        int colon = node.lastIndexOf(':');
        if (colon > 0 && colon < node.length() - 1) {
            try {
                int port = Integer.parseInt(node.substring(colon + 1));
                // Map 7201->1, 7202->2, etc. If different base, adjust here.
                int base = 7200;
                int derived = port - base;
                return Math.max(derived, 0);
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }

        // Pattern: "node-5", "zk-node-2" -> take number after last '-'
        int dash = node.lastIndexOf('-');
        if (dash >= 0 && dash < node.length() - 1) {
            try {
                return Integer.parseInt(node.substring(dash + 1));
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }

        // No parseable rank → lowest
        return 0;
    }
}
