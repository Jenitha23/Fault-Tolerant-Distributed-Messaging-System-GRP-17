package replication;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks processed message IDs to prevent duplicate processing.
 * Thread-safe and zero-config (no VM args).
 */
public class DeduplicationService {

    private final Set<String> seen = ConcurrentHashMap.newKeySet();

    /**
     * @return true if this messageId has already been processed before.
     */
    public boolean isDuplicate(String messageId) {
        // add() returns false if the value was already present.
        return !seen.add(messageId);
    }

    /** Optional: clear all tracked ids (e.g., in tests). */
    public void reset() {
        seen.clear();
    }

    /** @return number of unique message IDs tracked so far. */
    public int size() {
        return seen.size();
    }
}
