package org.tinystruct.workflow.repository;

import org.tinystruct.workflow.ExecutionContext;
import org.tinystruct.workflow.SnapshotIOException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link SnapshotRepository} backed by a {@link ConcurrentHashMap}.
 *
 * <p>Each save/load round-trips through JSON serialization to ensure callers
 * receive independent copies — mutations to a returned context do not affect
 * the stored snapshot, and vice versa. This keeps behaviour consistent with
 * persistent implementations.
 *
 * <p>Suitable for testing and single-node deployments. Not persistent across restarts.
 */
public class MemorySnapshotRepository implements SnapshotRepository {

    private final ConcurrentHashMap<String, String> snapshots = new ConcurrentHashMap<>();

    @Override
    public void save(ExecutionContext context) throws SnapshotIOException {
        if (context == null || context.getExecutionId() == null) {
            throw new SnapshotIOException("Cannot save null context or context with null execution ID");
        }
        snapshots.put(context.getExecutionId(), context.toJson());
    }

    @Override
    public ExecutionContext load(String executionId) throws SnapshotIOException {
        if (executionId == null) {
            throw new SnapshotIOException("executionId must not be null");
        }
        String json = snapshots.get(executionId);
        if (json == null) {
            return null;
        }
        try {
            return ExecutionContext.fromJson(json);
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to deserialize context for execution: " + executionId, e);
        }
    }

    @Override
    public void delete(String executionId) throws SnapshotIOException {
        if (executionId == null) {
            throw new SnapshotIOException("executionId must not be null");
        }
        snapshots.remove(executionId);
    }
}
