package org.tinystruct.workflow;

import org.tinystruct.ApplicationRuntimeException;
import org.tinystruct.system.Event;

/**
 * Thrown by {@link Workflow#suspend} to signal that the current node wants to
 * pause execution. This is a control-flow mechanism, not an error — it is caught
 * and handled by {@link ExecutionRuntime}.
 */
public class SuspendExecutionException extends ApplicationRuntimeException {
    private static final long serialVersionUID = 1L;

    private final String reason;
    private final Class<? extends Event<?>> waitingEventType;

    SuspendExecutionException(String reason, Class<? extends Event<?>> waitingEventType) {
        super("Workflow suspended: " + reason);
        this.reason = reason;
        this.waitingEventType = waitingEventType;
    }

    /** A human-readable description of why the workflow is suspended. */
    public String getReason() {
        return reason;
    }

    /**
     * The event type that will resume the workflow, or {@code null} if execution
     * must be resumed manually via {@link WorkflowEngine#resume(String)}.
     */
    public Class<? extends Event<?>> getWaitingEventType() {
        return waitingEventType;
    }
}
