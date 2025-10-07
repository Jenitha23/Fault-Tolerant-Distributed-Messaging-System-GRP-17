package common;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracks average latencies and system performance statistics
 * for messaging, replication, and leader elections.
 */
public class SystemMetrics {

    private final List<Long> messageDeliveryTimes = new ArrayList<>();
    private final List<Long> replicationLatencies = new ArrayList<>();
    private final List<Long> electionDurations = new ArrayList<>();

    // --- Recording methods ---
    public synchronized void recordMessageDelivery(long deliveryTime) {
        if (deliveryTime >= 0) messageDeliveryTimes.add(deliveryTime);
    }

    public synchronized void recordReplicationLatency(long latency) {
        if (latency >= 0) replicationLatencies.add(latency);
    }

    public synchronized void recordElectionDuration(long duration) {
        if (duration >= 0) electionDurations.add(duration);
    }

    // --- Printing summary ---
    public synchronized void printMetrics() {
        System.out.println("\n📊 SYSTEM METRICS SUMMARY 📊");

        System.out.printf("📨 Avg Message Delivery: %.2f ms (%d samples)%n",
                average(messageDeliveryTimes), messageDeliveryTimes.size());

        System.out.printf("🔄 Avg Replication Latency: %.2f ms (%d samples)%n",
                average(replicationLatencies), replicationLatencies.size());

        System.out.printf("👑 Avg Election Duration: %.2f ms (%d samples)%n",
                average(electionDurations), electionDurations.size());
    }

    // --- Helper: compute mean safely ---
    private static double average(List<Long> values) {
        if (values == null || values.isEmpty()) return 0.0;
        long sum = 0;
        for (long v : values) sum += v;
        return (double) sum / values.size();
    }
}
