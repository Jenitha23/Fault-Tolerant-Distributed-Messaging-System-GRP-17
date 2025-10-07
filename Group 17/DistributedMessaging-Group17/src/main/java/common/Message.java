package common;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Basic message envelope used across the system.
 * Mutable by design (timestamps are set by time sync / sequencer).
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String id;
    private final String sender;
    private final String receiver;
    private final String content;

    private long timestamp;         // wall-clock (ms)
    private long logicalTimestamp;  // lamport/hybrid logical time
    private VectorClock vectorClock; // optional; can be null if not used

    public Message(String sender, String receiver, String content) {
        this(UUID.randomUUID().toString(), sender, receiver, content,
             System.currentTimeMillis(), 0L, null);
    }

    public Message(String id,
                   String sender,
                   String receiver,
                   String content,
                   long timestamp,
                   long logicalTimestamp,
                   VectorClock vectorClock) {
        this.id = (id == null || id.isBlank()) ? UUID.randomUUID().toString() : id;
        this.sender = nonEmpty(sender, "sender");
        this.receiver = nonEmpty(receiver, "receiver");
        this.content = nonEmpty(content, "content");
        this.timestamp = timestamp > 0 ? timestamp : System.currentTimeMillis();
        this.logicalTimestamp = Math.max(0L, logicalTimestamp);
        this.vectorClock = vectorClock; // may be null; getter guards it
    }

    private static String nonEmpty(String v, String field) {
        if (v == null) throw new IllegalArgumentException(field + " cannot be null");
        String t = v.trim();
        if (t.isEmpty()) throw new IllegalArgumentException(field + " cannot be empty");
        return t;
    }

    // Getters
    public String getId() { return id; }
    public String getSender() { return sender; }
    public String getReceiver() { return receiver; }
    public String getContent() { return content; }
    public long getTimestamp() { return timestamp; }
    public long getLogicalTimestamp() { return logicalTimestamp; }

    /**
     * Returns a non-null VectorClock. If none was set, returns an empty one.
     * This avoids NPEs in callers that expect a clock.
     */
    public VectorClock getVectorClock() {
        if (vectorClock == null) vectorClock = new VectorClock();
        return vectorClock;
    }

    // Setters used by time sync / sequencing stages
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp > 0 ? timestamp : System.currentTimeMillis();
    }

    public void setLogicalTimestamp(long logicalTimestamp) {
        this.logicalTimestamp = Math.max(0L, logicalTimestamp);
    }

    public void setVectorClock(VectorClock vectorClock) {
        this.vectorClock = vectorClock; // allow null; getter guards it
    }

    // Helpers
    public String shortId() {
        return id.length() >= 8 ? id.substring(0, 8) : id;
    }

    @Override
    public String toString() {
        return String.format("Message[%s: %s -> %s: %s]",
                shortId(), sender, receiver, content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id); // id is unique
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Message)) return false;
        Message other = (Message) obj;
        return Objects.equals(this.id, other.id);
    }
}
