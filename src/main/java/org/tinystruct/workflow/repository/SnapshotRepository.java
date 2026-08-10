package org.tinystruct.workflow.repository;

import org.tinystruct.workflow.ExecutionContext;
import org.tinystruct.workflow.SnapshotIOException;

/**
 * Persistence contract for workflow execution snapshots.
 *
 * <p>Each call to {@link #save} must persist a complete, independent copy of the context
 * so that a subsequent {@link #load} reflects the state at the time of the save, not
 * any later in-memory mutations.
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
}
