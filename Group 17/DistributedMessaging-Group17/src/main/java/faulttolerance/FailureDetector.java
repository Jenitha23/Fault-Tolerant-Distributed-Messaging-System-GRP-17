package faulttolerance;

import faulttolerance.FaultToleranceManager;
import java.util.List;

/**
 * Adapter that preserves the old class name but delegates to the new
 * FaultToleranceManager (which already implements debounced state-change events).
 */
public class FailureDetector {

    public interface Listener extends FaultToleranceManager.Listener {}

    private final FaultToleranceManager inner = new FaultToleranceManager();

    public FailureDetector(List<String> peers, Listener listener) {
        inner.setNodes(peers);
        inner.setListener(listener);
    }

    public void start() {
        inner.startFailureDetection();
    }

    public void stop() {
        inner.stop();
    }
}
