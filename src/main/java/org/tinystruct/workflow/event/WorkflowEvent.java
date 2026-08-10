package org.tinystruct.workflow.event;

import org.tinystruct.system.Event;

/**
 * Base class for events that can resume a suspended workflow execution.
 *
 * <p>Subclass this to define domain-specific events:
 * <pre>
 * public class ApprovalEvent extends WorkflowEvent&lt;Boolean&gt; {
 *     public ApprovalEvent(String executionId, boolean approved) {
 *         super("ApprovalEvent", executionId, approved);
 *     }
 * }
 * </pre>
 *
 * @param <T> the type of the event payload
 */
public abstract class WorkflowEvent<T> implements Event<T> {

    private final String name;
    private final String executionId;
    private final T payload;

    protected WorkflowEvent(String name, String executionId, T payload) {
        this.name = name;
        this.executionId = executionId;
        this.payload = payload;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public T getPayload() {
        return payload;
    }

    /** The execution ID of the workflow this event is targeting. */
    public String getExecutionId() {
        return executionId;
    }
}
