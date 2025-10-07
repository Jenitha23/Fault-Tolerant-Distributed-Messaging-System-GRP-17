package timesync;

import common.Message;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class MessageSequencer {

    private final Map<String, PriorityBlockingQueue<SequencedMessage>> conversationQueues = new ConcurrentHashMap<>();
    private final Map<String, Long> lastDeliveredSequence = new ConcurrentHashMap<>();

    public void queueMessage(Message message) {
        String conversationId = getConversationId(message);
        long sequence = message.getLogicalTimestamp();

        conversationQueues
            .computeIfAbsent(conversationId, k -> new PriorityBlockingQueue<>(11,
                Comparator.comparingLong(SequencedMessage::getSequence)))
            .add(new SequencedMessage(message, sequence));

        deliverOrderedMessages(conversationId);
    }

    private void deliverOrderedMessages(String conversationId) {
        PriorityBlockingQueue<SequencedMessage> queue = conversationQueues.get(conversationId);
        if (queue == null) return;

        long expectedSequence = lastDeliveredSequence.getOrDefault(conversationId, 0L) + 1;

        while (!queue.isEmpty() && queue.peek() != null && queue.peek().getSequence() == expectedSequence) {
            SequencedMessage sequencedMessage = queue.poll();
            if (sequencedMessage != null) {
                deliverMessage(sequencedMessage.getMessage());
                lastDeliveredSequence.put(conversationId, expectedSequence);
                expectedSequence++;
            }
        }
    }

    private void deliverMessage(Message message) {
        System.out.println("📨 DELIVERED [" + message.getLogicalTimestamp() + "] " +
                message.getSender() + " -> " + message.getReceiver() + ": " + message.getContent());
    }

    private String getConversationId(Message message) {
        String[] participants = { message.getSender(), message.getReceiver() };
        Arrays.sort(participants);
        return participants[0] + "-" + participants[1];
    }

    public void reorderMessages(List<Message> messages) {
        messages.sort(Comparator.comparingLong(Message::getLogicalTimestamp));
        System.out.println("🔄 Reordered " + messages.size() + " messages by logical timestamp");
    }

    private static class SequencedMessage {
        private final Message message;
        private final long sequence;

        public SequencedMessage(Message message, long sequence) {
            this.message = message;
            this.sequence = sequence;
        }

        public Message getMessage() { return message; }
        public long getSequence() { return sequence; }
    }
}
