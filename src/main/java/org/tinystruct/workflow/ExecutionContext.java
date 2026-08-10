package org.tinystruct.workflow;

import org.tinystruct.ApplicationException;
import org.tinystruct.data.component.Builder;

import java.io.Serializable;

/**
 * Holds the runtime state of a single workflow execution.
 * Serialized to/from JSON for persistence in a {@link org.tinystruct.workflow.repository.SnapshotRepository}.
 */
public class ExecutionContext implements Serializable {
    private static final long serialVersionUID = 1L;

    private String workflowId;
    private String executionId;
    private int currentNodeIndex;
    private Builder variables = new Builder();
    private WorkflowStatus status = WorkflowStatus.NEW;
    private String suspendReason;
    private String waitingEventType;
    private String failureMessage;
    private long createdTime;
    private long updatedTime;

    public ExecutionContext() {
        long now = System.currentTimeMillis();
        this.createdTime = now;
        this.updatedTime = now;
    }

    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }

    public String getExecutionId() { return executionId; }
    public void setExecutionId(String executionId) { this.executionId = executionId; }

    public int getCurrentNodeIndex() { return currentNodeIndex; }
    public void setCurrentNodeIndex(int currentNodeIndex) { this.currentNodeIndex = currentNodeIndex; }

    public Builder getVariables() { return variables; }
    public void setVariables(Builder variables) { this.variables = variables; }

    public WorkflowStatus getStatus() { return status; }
    public void setStatus(WorkflowStatus status) { this.status = status; }

    public String getSuspendReason() { return suspendReason; }
    public void setSuspendReason(String suspendReason) { this.suspendReason = suspendReason; }

    /** Fully-qualified class name of the event type this execution is waiting for, or {@code null}. */
    public String getWaitingEventType() { return waitingEventType; }
    public void setWaitingEventType(String waitingEventType) { this.waitingEventType = waitingEventType; }

    /** The error message from the node that caused this execution to fail, or {@code null}. */
    public String getFailureMessage() { return failureMessage; }
    public void setFailureMessage(String failureMessage) { this.failureMessage = failureMessage; }

    public long getCreatedTime() { return createdTime; }
    public void setCreatedTime(long createdTime) { this.createdTime = createdTime; }

    public long getUpdatedTime() { return updatedTime; }
    public void setUpdatedTime(long updatedTime) { this.updatedTime = updatedTime; }

    /** Serializes this context to a JSON string. */
    public String toJson() {
        Builder builder = new Builder();
        builder.put("workflowId", workflowId);
        builder.put("executionId", executionId);
        builder.put("currentNodeIndex", currentNodeIndex);
        builder.put("variables", variables);
        builder.put("status", status.name());
        builder.put("suspendReason", suspendReason);
        builder.put("waitingEventType", waitingEventType);
        builder.put("failureMessage", failureMessage);
        builder.put("createdTime", createdTime);
        builder.put("updatedTime", updatedTime);
        return builder.toString();
    }

    /** Deserializes a context from a JSON string produced by {@link #toJson()}. */
    public static ExecutionContext fromJson(String json) throws ApplicationException {
        Builder builder = new Builder();
        builder.parse(json);

        ExecutionContext context = new ExecutionContext();

        if (builder.containsKey("workflowId"))
            context.setWorkflowId(builder.get("workflowId").toString());
        if (builder.containsKey("executionId"))
            context.setExecutionId(builder.get("executionId").toString());
        if (builder.containsKey("currentNodeIndex"))
            context.setCurrentNodeIndex(Integer.parseInt(builder.get("currentNodeIndex").toString()));

        if (builder.containsKey("variables")) {
            Object vars = builder.get("variables");
            if (vars instanceof Builder) {
                context.setVariables((Builder) vars);
            } else if (vars != null) {
                Builder varsBuilder = new Builder();
                varsBuilder.parse(vars.toString());
                context.setVariables(varsBuilder);
            }
        }

        if (builder.containsKey("status"))
            context.setStatus(WorkflowStatus.valueOf(builder.get("status").toString()));

        if (builder.containsKey("suspendReason") && builder.get("suspendReason") != null)
            context.setSuspendReason(builder.get("suspendReason").toString());

        if (builder.containsKey("waitingEventType") && builder.get("waitingEventType") != null)
            context.setWaitingEventType(builder.get("waitingEventType").toString());

        if (builder.containsKey("failureMessage") && builder.get("failureMessage") != null)
            context.setFailureMessage(builder.get("failureMessage").toString());

        if (builder.containsKey("createdTime"))
            context.setCreatedTime(Long.parseLong(builder.get("createdTime").toString()));
        if (builder.containsKey("updatedTime"))
            context.setUpdatedTime(Long.parseLong(builder.get("updatedTime").toString()));

        return context;
    }
}
