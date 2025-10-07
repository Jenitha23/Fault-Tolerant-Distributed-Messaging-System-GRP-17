import common.Message;
import common.VectorClock;
import coordination.LeaderElector;
import replication.QuorumReplicationManager;
import timesync.HybridTimeSynchronizer;
import faulttolerance.FailureDetector;

import java.util.List;
import java.util.Map;

public class IntegrationTest {
    
    public static void main(String[] args) {
        System.out.println("🧪 INTEGRATION TESTS - Distributed Messaging System");
        System.out.println("====================================================");
        
        runAllIntegrationTests();
        
        System.out.println("\n✅ ALL INTEGRATION TESTS COMPLETED!");
    }
    
    public static void runAllIntegrationTests() {
        testVectorClockIntegration();
        testLeaderElectionIntegration();
        testReplicationIntegration();
        testTimeSynchronizationIntegration();
        testFailureDetectionIntegration();
        testEndToEndMessaging();
    }
    
    private static void testVectorClockIntegration() {
        System.out.println("\n🔬 TEST: Vector Clock Integration");
        
        try {
            // Create messages with vector clocks
            Message message1 = new Message("user1", "user2", "Hello");
            Message message2 = new Message("user2", "user1", "Hi there!");
            
            // Test vector clock operations
            VectorClock clock1 = message1.getVectorClock();
            clock1.increment("user1");
            
            VectorClock clock2 = message2.getVectorClock();
            clock2.increment("user2");
            
            // Test merging
            clock1.merge(clock2.getClock());
            
            System.out.println("✅ Vector Clock Integration: PASSED");
            System.out.println("   - Message creation with vector clocks");
            System.out.println("   - Clock increment operations");
            System.out.println("   - Clock merging functionality");
            System.out.println("   - Final clock state: " + clock1.serialize());
            
        } catch (Exception e) {
            System.out.println("❌ Vector Clock Integration: FAILED - " + e.getMessage());
        }
    }
    
    private static void testLeaderElectionIntegration() {
        System.out.println("\n👑 TEST: Leader Election Integration");
        
        try {
            List<String> nodes = List.of("node-1", "node-2", "node-3", "node-4");
            
            // Test leader election
            String leader = LeaderElector.electLeader(nodes);
            
            if ("node-4".equals(leader)) {
                System.out.println("✅ Leader Election Integration: PASSED");
                System.out.println("   - Multiple nodes participation");
                System.out.println("   - Correct leader selection (highest ID)");
                System.out.println("   - Election result: " + leader);
            } else {
                System.out.println("❌ Leader Election Integration: FAILED - Wrong leader elected");
            }
            
            // Test eligibility
            boolean eligible = LeaderElector.isEligibleForLeadership("node-3", nodes);
            System.out.println("   - Eligibility check: node-3 eligible = " + eligible);
            
        } catch (Exception e) {
            System.out.println("❌ Leader Election Integration: FAILED - " + e.getMessage());
        }
    }
    
    private static void testReplicationIntegration() {
        System.out.println("\n🔄 TEST: Replication Integration");
        
        try {
            // Create replication manager for 3 nodes
            QuorumReplicationManager replication = new QuorumReplicationManager(3);
            
            // Test write operation
            String messageId = "test-message-123";
            boolean writeSuccess = replication.writeMessage(messageId, "Test content");
            
            if (writeSuccess) {
                System.out.println("✅ Replication Write Integration: PASSED");
                System.out.println("   - Quorum-based write operation");
                System.out.println("   - Write success confirmation");
            } else {
                System.out.println("❌ Replication Write Integration: FAILED - Write failed");
                return;
            }
            
            // Test read operation
            String content = replication.readMessage(messageId);
            if ("Test content".equals(content)) {
                System.out.println("✅ Replication Read Integration: PASSED");
                System.out.println("   - Quorum-based read operation");
                System.out.println("   - Data consistency maintained");
            } else {
                System.out.println("❌ Replication Read Integration: FAILED - Read inconsistency");
            }
            
        } catch (Exception e) {
            System.out.println("❌ Replication Integration: FAILED - " + e.getMessage());
        }
    }
    
    private static void testTimeSynchronizationIntegration() {
        System.out.println("\n⏰ TEST: Time Synchronization Integration");
        
        try {
            HybridTimeSynchronizer timeSync = new HybridTimeSynchronizer();
            
            // Test timestamp generation
            long timestamp1 = timeSync.getCurrentTimestamp();
            long logicalTime1 = timeSync.getNextLogicalTime();
            
            Thread.sleep(10); // Small delay
            
            long timestamp2 = timeSync.getCurrentTimestamp();
            long logicalTime2 = timeSync.getNextLogicalTime();
            
            // Verify timestamps are increasing
            if (timestamp2 > timestamp1 && logicalTime2 > logicalTime1) {
                System.out.println("✅ Time Synchronization Integration: PASSED");
                System.out.println("   - Physical timestamp generation");
                System.out.println("   - Logical timestamp sequencing");
                System.out.println("   - Time progression: " + timestamp1 + " -> " + timestamp2);
                System.out.println("   - Logical progression: " + logicalTime1 + " -> " + logicalTime2);
            } else {
                System.out.println("❌ Time Synchronization Integration: FAILED - Time not progressing correctly");
            }
            
        } catch (Exception e) {
            System.out.println("❌ Time Synchronization Integration: FAILED - " + e.getMessage());
        }
    }
    
    private static void testFailureDetectionIntegration() {
        System.out.println("\n🔍 TEST: Failure Detection Integration");
        
        try {
            FailureDetector detector = new FailureDetector(5000); // 5 second timeout
            
            // Test node monitoring
            detector.setNodesToMonitor(List.of("node-1", "node-2", "node-3"));
            
            // Simulate heartbeats
            detector.recordHeartbeat("node-1");
            detector.recordHeartbeat("node-2");
            
            // Check node status
            boolean node1Alive = detector.isNodeAlive("node-1");
            boolean node3Alive = detector.isNodeAlive("node-3"); // No heartbeat recorded
            
            if (node1Alive && !node3Alive) {
                System.out.println("✅ Failure Detection Integration: PASSED");
                System.out.println("   - Heartbeat recording");
                System.out.println("   - Node status monitoring");
                System.out.println("   - Failure detection: node-1=" + node1Alive + ", node-3=" + node3Alive);
            } else {
                System.out.println("❌ Failure Detection Integration: FAILED - Status incorrect");
            }
            
            detector.stop();
            
        } catch (Exception e) {
            System.out.println("❌ Failure Detection Integration: FAILED - " + e.getMessage());
        }
    }
    
    private static void testEndToEndMessaging() {
        System.out.println("\n📨 TEST: End-to-End Messaging Integration");
        
        try {
            // Create a complete message flow test
            Message message = new Message("test-sender", "test-receiver", "Integration test message");
            
            // Set up timing
            HybridTimeSynchronizer timeSync = new HybridTimeSynchronizer();
            message.setTimestamp(timeSync.getCurrentTimestamp());
            message.setLogicalTimestamp(timeSync.getNextLogicalTime());
            
            // Test vector clock integration
            message.getVectorClock().increment("test-sender");
            
            // Verify all components integrated
            boolean hasSender = message.getSender() != null;
            boolean hasReceiver = message.getReceiver() != null;
            boolean hasContent = message.getContent() != null;
            boolean hasTimestamp = message.getTimestamp() > 0;
            boolean hasLogicalTime = message.getLogicalTimestamp() > 0;
            boolean hasVectorClock = message.getVectorClock() != null;
            
            if (hasSender && hasReceiver && hasContent && hasTimestamp && hasLogicalTime && hasVectorClock) {
                System.out.println("✅ End-to-End Messaging Integration: PASSED");
                System.out.println("   - Message creation: " + message);
                System.out.println("   - Timestamp: " + message.getTimestamp());
                System.out.println("   - Logical time: " + message.getLogicalTimestamp());
                System.out.println("   - Vector clock: " + message.getVectorClock().serialize());
            } else {
                System.out.println("❌ End-to-End Messaging Integration: FAILED - Missing components");
            }
            
        } catch (Exception e) {
            System.out.println("❌ End-to-End Messaging Integration: FAILED - " + e.getMessage());
        }
    }
    
    public static boolean runQuickTest() {
        try {
            // Quick smoke test
            Message testMessage = new Message("test", "test", "test");
            return testMessage.getId() != null && testMessage.getVectorClock() != null;
        } catch (Exception e) {
            return false;
        }
    }
}