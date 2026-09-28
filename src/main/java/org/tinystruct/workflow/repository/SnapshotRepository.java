package org.tinystruct.workflow.repository;

import org.tinystruct.workflow.ExecutionContext;
import org.tinystruct.workflow.SnapshotIOException;
import org.tinystruct.workflow.WorkflowStatus;

import java.util.List;

/**
 * Persistence contract for workflow execution snapshots.
 *
 * <p>Each call to {@link #save} must persist a complete, independent copy of the context
 * so that a subsequent {@link #load} reflects the state at the time of the save, not
 * any later in-memory mutations.
 *
 * <p>Two operations exist for callers that cannot rely on a single process owning the storage:
 * {@link #compareAndSetStatus} claims an execution without a race, and {@link #findByStatus}
 * enumerates executions so that abandoned ones can be found and cleaned up. Both have defaults so
 * that existing implementations still compile; both defaults are weaker than the contract and say so.
 */
public interface SnapshotRepository {

    /**
     * Persists the given execution context.
     *
     * @throws SnapshotIOException if the context or its execution ID is null, or on I/O failure
     */
    void save(ExecutionContext context) throws SnapshotIOException;

    /**
     * Loads the execution context for the given ID.
     *
     * @return the context, or {@code null} if not found
     * @throws SnapshotIOException if executionId is null or on I/O failure
     */
    ExecutionContext load(String executionId) throws SnapshotIOException;

    /**
     * Deletes the snapshot for the given execution ID.
     * Safe to call even if the ID does not exist.
     *
     * @throws SnapshotIOException if executionId is null or on I/O failure
     */
    void delete(String executionId) throws SnapshotIOException;

    /**
     * Atomically moves an execution from {@code expected} to {@code target}, and persists the new
     * status. Exactly one caller may win, however many processes or hosts attempt it at once.
     *
     * <p>This is how an execution is claimed before it is resumed. A caller that gets {@code false}
     * must not touch the execution: someone else owns it, or it is no longer in {@code expected}.
     *
     * <p><strong>The default is not atomic.</strong> It reads, checks and writes, so two callers can
     * both win. Implementations that can do better — a conditional {@code UPDATE}, a Redis
     * transaction, a file lock — must override it; the ones in this package do.
     *
     * @return {@code true} if this caller made the transition
     * @throws SnapshotIOException if executionId or either status is null, or on I/O failure
     */
    default boolean compareAndSetStatus(String executionId, WorkflowStatus expected, WorkflowStatus target)
            throws SnapshotIOException {
        if (executionId == null || expected == null || target == null) {
            throw new SnapshotIOException("executionId, expected and target must not be null");
        }
        ExecutionContext context = load(executionId);
        if (context == null || context.getStatus() != expected) {
            return false;
        }
        context.setStatus(target);
        context.setUpdatedTime(System.currentTimeMillis());
        save(context);
        return true;
    }

    /**
     * Every execution currently in the given status, in no particular order.
     *
     * <p>Callers use this to find work that nothing is going to finish on its own — an execution
     * left {@code WAITING} for an answer that never came, whose snapshot still holds its variables.
     * Without it such a snapshot can only be deleted by someone who already knows its id.
     *
     * <p>The default refuses rather than returning an empty list, because a caller sweeping for
     * abandoned executions must be able to tell "none" from "this repository cannot look".
     *
     * @throws UnsupportedOperationException if this repository cannot enumerate its snapshots
     * @throws SnapshotIOException           if status is null or on I/O failure
     */
    default List<ExecutionContext> findByStatus(WorkflowStatus status) throws SnapshotIOException {
        throw new UnsupportedOperationException(getClass().getName() + " cannot enumerate snapshots; "
                + "override findByStatus to let callers find and clean up abandoned executions.");
    }
}
