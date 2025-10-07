package common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread-safe implementation of a Vector Clock.
 * Used to track causal relationships between distributed events.
 */
public class VectorClock {

    private final Map<String, Integer> clock;

    public VectorClock() {
        this.clock = new ConcurrentHashMap<>();
    }

    public VectorClock(Map<String, Integer> initialClock) {
        this.clock = new ConcurrentHashMap<>(initialClock);
    }

    /** Increment this node's clock by 1. */
    public void increment(String nodeId) {
        clock.put(nodeId, clock.getOrDefault(nodeId, 0) + 1);
    }

    /** Merge another vector clock into this one (taking element-wise max). */
    public void mergeWith(Map<String, Integer> otherClock) {
        if (otherClock == null) return;
        for (Map.Entry<String, Integer> entry : otherClock.entrySet()) {
            String nodeId = entry.getKey();
            int mergedValue = Math.max(clock.getOrDefault(nodeId, 0), entry.getValue());
            clock.put(nodeId, mergedValue);
        }
    }

    /** Serialize the vector clock into a simple key:value;key:value string. */
    public String serialize() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> entry : clock.entrySet()) {
            if (sb.length() > 0) sb.append(';');
            sb.append(entry.getKey()).append(':').append(entry.getValue());
        }
        return sb.toString();
    }

    /** Deserialize a string into a Map-based clock. */
    public static Map<String, Integer> deserialize(String data) {
        Map<String, Integer> map = new HashMap<>();
        if (data == null || data.isEmpty()) return map;

        String[] pairs = data.split(";");
        for (String pair : pairs) {
            String[] parts = pair.split(":");
            if (parts.length == 2) {
                try {
                    map.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                } catch (NumberFormatException ignore) {
                    // skip malformed entries
                }
            }
        }
        return map;
    }

    /** Return a safe copy of the clock map. */
    public Map<String, Integer> getClock() {
        return new HashMap<>(clock);
    }

    /**
     * Compare this vector clock with another.
     * @return -1 if this < other, 0 if concurrent, 1 if this > other.
     */
    public int compareTo(VectorClock other) {
        boolean greater = false;
        boolean less = false;

        for (String nodeId : clock.keySet()) {
            int thisVal = clock.getOrDefault(nodeId, 0);
            int otherVal = other.clock.getOrDefault(nodeId, 0);
            if (thisVal > otherVal) greater = true;
            else if (thisVal < otherVal) less = true;
        }

        if (greater && !less) return 1;
        if (less && !greater) return -1;
        return 0; // concurrent
    }

    @Override
    public String toString() {
        return serialize();
    }
}
