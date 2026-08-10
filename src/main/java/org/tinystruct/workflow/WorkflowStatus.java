package org.tinystruct.workflow;

/**
 * Represents the lifecycle states of a workflow execution.
 *
 * <pre>
 *   NEW → RUNNING → WAITING → RUNNING → COMPLETED
 *                           ↘ FAILED
 *                           ↘ CANCELLED
 * </pre>
 */
public enum WorkflowStatus {
    /** Execution has been created but not yet started. */
    NEW,
    /** Execution is actively processing a node. */
    RUNNING,
    /** Execution is suspended, waiting for an external event. */
    WAITING,
    /** Execution finished all nodes successfully. */
    COMPLETED,
    /** Execution failed due to an unhandled exception in a node. */
    FAILED,
    /** Execution was explicitly cancelled. */
    CANCELLED
}
