package org.tinystruct.workflow;

import org.tinystruct.system.Event;

/**
 * Static API for workflow node actions to interact with the current execution context.
 *
 * <p>These methods are only valid when called from within a node action during
 * workflow execution. Calling them outside that scope has no effect (reads return
 * {@code null}, writes are silently ignored).
 *
 * <pre>
 * // Suspend until a specific event arrives
 * Workflow.suspend("Waiting for approval", ApprovalEvent.class);
 *
 * // Share data between nodes
 * Workflow.setVariable("orderId", "ORD-123");
 * String orderId = (String) Workflow.getVariable("orderId");
 * </pre>
 */
public final class Workflow {

    /** The execution context bound to the current execution thread. */
    static final ThreadLocal<ExecutionContext> CURRENT = new ThreadLocal<>();

    private Workflow() {}

    /**
     * Suspends the current workflow execution without waiting for an event.
     * The workflow remains WAITING until manually resumed via
     * {@link WorkflowEngine#resume(String)}.
     *
     * @param reason a human-readable description of why execution is paused
     */
    public static void suspend(String reason) {
        throw new SuspendExecutionException(reason, null);
    }

    /**
     * Suspends the current workflow execution and waits for an event of the given type.
     * Execution resumes automatically when a matching event is dispatched via
     * {@link org.tinystruct.system.EventDispatcher}.
     *
     * @param reason        a human-readable description of why execution is paused
     * @param waitingEventType the event class that will resume this execution
     */
    public static void suspend(String reason, Class<? extends Event<?>> waitingEventType) {
        throw new SuspendExecutionException(reason, waitingEventType);
    }

    /**
     * Returns a variable stored in the current execution context.
     *
     * @param key the variable name
     * @return the value, or {@code null} if not set or called outside a workflow node
     */
    public static Object getVariable(String key) {
        ExecutionContext context = CURRENT.get();
        return context != null ? context.getVariables().get(key) : null;
    }

    /**
     * Stores a variable in the current execution context, making it available
     * to all subsequent nodes in the same execution.
     *
     * @param key   the variable name
     * @param value the value to store
     */
    public static void setVariable(String key, Object value) {
        ExecutionContext context = CURRENT.get();
        if (context != null) {
            context.getVariables().put(key, value);
        }
    }
}
