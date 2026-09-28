package org.tinystruct.workflow.repository;

import org.tinystruct.workflow.ExecutionContext;
import org.tinystruct.workflow.SnapshotIOException;
import org.tinystruct.workflow.WorkflowStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

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

    /** Atomic through {@link ConcurrentHashMap#computeIfPresent}, which holds the bin lock. */
    @Override
    public boolean compareAndSetStatus(String executionId, WorkflowStatus expected, WorkflowStatus target)
            throws SnapshotIOException {
        if (executionId == null || expected == null || target == null) {
            throw new SnapshotIOException("executionId, expected and target must not be null");
        }
        AtomicBoolean changed = new AtomicBoolean();
        SnapshotIOException[] failure = new SnapshotIOException[1];
        snapshots.computeIfPresent(executionId, (id, json) -> {
            try {
                ExecutionContext context = ExecutionContext.fromJson(json);
                if (context.getStatus() != expected) {
                    return json;
                }
                context.setStatus(target);
                context.setUpdatedTime(System.currentTimeMillis());
                changed.set(true);
                return context.toJson();
            } catch (Exception e) {
                failure[0] = new SnapshotIOException("Failed to update status for execution: " + id, e);
                return json;
            }
        });
        if (failure[0] != null) {
            throw failure[0];
        }
        return changed.get();
    }

    @Override
    public List<ExecutionContext> findByStatus(WorkflowStatus status) throws SnapshotIOException {
        if (status == null) {
            throw new SnapshotIOException("status must not be null");
        }
        List<ExecutionContext> found = new ArrayList<>();
        for (String json : snapshots.values()) {
            try {
                ExecutionContext context = ExecutionContext.fromJson(json);
                if (context.getStatus() == status) {
                    found.add(context);
                }
            } catch (Exception e) {
                throw new SnapshotIOException("Failed to deserialize a stored context", e);
            }
        }
        return found;
    }
}
