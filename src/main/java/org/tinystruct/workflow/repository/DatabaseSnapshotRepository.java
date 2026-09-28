package org.tinystruct.workflow.repository;

import org.tinystruct.data.DatabaseOperator;
import org.tinystruct.data.component.Builder;
import org.tinystruct.workflow.ExecutionContext;
import org.tinystruct.workflow.SnapshotIOException;
import org.tinystruct.workflow.WorkflowStatus;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * JDBC-backed {@link SnapshotRepository}.
 *
 * <p>Call {@link #initialize()} once at application startup to ensure the
 * {@code workflow_snapshots} table exists before the engine starts processing.
 *
 * <p>Saves use an upsert pattern (UPDATE then INSERT if no rows affected) to
 * avoid a separate existence check on every write.
 */
public class DatabaseSnapshotRepository implements SnapshotRepository {

    private static final Logger logger = Logger.getLogger(DatabaseSnapshotRepository.class.getName());

    private static final String DDL =
            "CREATE TABLE IF NOT EXISTS workflow_snapshots (" +
            "  execution_id       VARCHAR(36)  PRIMARY KEY," +
            "  workflow_id        VARCHAR(128) NOT NULL," +
            "  node_index         INT          NOT NULL," +
            "  status             VARCHAR(16)  NOT NULL," +
            "  variables          TEXT," +
            "  suspend_reason     TEXT," +
            "  waiting_event_type VARCHAR(256)," +
            "  failure_message    TEXT," +
            "  created_time       BIGINT," +
            "  updated_time       BIGINT" +
            ")";

    /**
     * Creates the {@code workflow_snapshots} table if it does not already exist.
     * Call this once during application startup.
     *
     * @throws SnapshotIOException if the DDL cannot be executed
     */
    public void initialize() throws SnapshotIOException {
        try (DatabaseOperator operator = new DatabaseOperator()) {
            operator.executeUpdate(operator.preparedStatement(DDL, new Object[]{}));
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to initialize workflow_snapshots table", e);
        }
    }

    @Override
    public void save(ExecutionContext context) throws SnapshotIOException {
        if (context == null || context.getExecutionId() == null) {
            throw new SnapshotIOException("Cannot save null context or context with null execution ID");
        }

        String vars = context.getVariables() != null ? context.getVariables().toString() : "{}";

        // Try UPDATE first; if no rows affected, INSERT
        try (DatabaseOperator operator = new DatabaseOperator()) {
            String updateSql =
                    "UPDATE workflow_snapshots SET workflow_id=?, node_index=?, status=?, variables=?," +
                    " suspend_reason=?, waiting_event_type=?, failure_message=?, updated_time=? WHERE execution_id=?";
            PreparedStatement updateStatement = operator.preparedStatement(updateSql, new Object[]{
                    context.getWorkflowId(),
                    context.getCurrentNodeIndex(),
                    context.getStatus().name(),
                    vars,
                    context.getSuspendReason(),
                    context.getWaitingEventType(),
                    context.getFailureMessage(),
                    context.getUpdatedTime(),
                    context.getExecutionId()
            });
            int rows = operator.executeUpdate(updateStatement);

            if (rows == 0) {
                String insertSql =
                        "INSERT INTO workflow_snapshots" +
                        " (execution_id, workflow_id, node_index, status, variables," +
                        "  suspend_reason, waiting_event_type, failure_message, created_time, updated_time)" +
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement insertStatement = operator.preparedStatement(insertSql, new Object[]{
                        context.getExecutionId(),
                        context.getWorkflowId(),
                        context.getCurrentNodeIndex(),
                        context.getStatus().name(),
                        vars,
                        context.getSuspendReason(),
                        context.getWaitingEventType(),
                        context.getFailureMessage(),
                        context.getCreatedTime(),
                        context.getUpdatedTime()
                });
                operator.executeUpdate(insertStatement);
            }
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to save snapshot for execution: " + context.getExecutionId(), e);
        }
    }

    @Override
    public ExecutionContext load(String executionId) throws SnapshotIOException {
        if (executionId == null) {
            throw new SnapshotIOException("executionId must not be null");
        }
        try (DatabaseOperator operator = new DatabaseOperator()) {
            PreparedStatement ps = operator.preparedStatement(
                    "SELECT execution_id, workflow_id, node_index, status, variables," +
                    " suspend_reason, waiting_event_type, failure_message, created_time, updated_time" +
                    " FROM workflow_snapshots WHERE execution_id = ?",
                    new Object[]{executionId});
            try (ResultSet rs = operator.executeQuery(ps)) {
                if (!rs.next()) return null;
                return map(rs);
            }
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to load snapshot for execution: " + executionId, e);
        }
    }

    @Override
    public void delete(String executionId) throws SnapshotIOException {
        if (executionId == null) {
            throw new SnapshotIOException("executionId must not be null");
        }
        try (DatabaseOperator operator = new DatabaseOperator()) {
            operator.executeUpdate(operator.preparedStatement(
                    "DELETE FROM workflow_snapshots WHERE execution_id = ?",
                    new Object[]{executionId}));
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to delete snapshot for execution: " + executionId, e);
        }
    }

    /**
     * A single conditional {@code UPDATE}: the database decides the winner, so this holds across
     * processes and hosts. One row affected means this caller made the transition.
     */
    @Override
    public boolean compareAndSetStatus(String executionId, WorkflowStatus expected, WorkflowStatus target)
            throws SnapshotIOException {
        if (executionId == null || expected == null || target == null) {
            throw new SnapshotIOException("executionId, expected and target must not be null");
        }
        try (DatabaseOperator operator = new DatabaseOperator()) {
            PreparedStatement ps = operator.preparedStatement(
                    "UPDATE workflow_snapshots SET status = ?, updated_time = ?" +
                    " WHERE execution_id = ? AND status = ?",
                    new Object[]{target.name(), System.currentTimeMillis(), executionId, expected.name()});
            return operator.executeUpdate(ps) == 1;
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to claim execution: " + executionId, e);
        }
    }

    @Override
    public List<ExecutionContext> findByStatus(WorkflowStatus status) throws SnapshotIOException {
        if (status == null) {
            throw new SnapshotIOException("status must not be null");
        }
        List<ExecutionContext> found = new ArrayList<>();
        try (DatabaseOperator operator = new DatabaseOperator()) {
            PreparedStatement ps = operator.preparedStatement(
                    "SELECT execution_id, workflow_id, node_index, status, variables," +
                    " suspend_reason, waiting_event_type, failure_message, created_time, updated_time" +
                    " FROM workflow_snapshots WHERE status = ?",
                    new Object[]{status.name()});
            try (ResultSet rs = operator.executeQuery(ps)) {
                while (rs.next()) {
                    found.add(map(rs));
                }
            }
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to list snapshots with status: " + status, e);
        }
        return found;
    }

    /** Maps the current row of a full-column select onto an {@link ExecutionContext}. */
    private static ExecutionContext map(ResultSet rs) throws SQLException, org.tinystruct.ApplicationException {
        ExecutionContext context = new ExecutionContext();
        context.setExecutionId(rs.getString("execution_id"));
        context.setWorkflowId(rs.getString("workflow_id"));
        context.setCurrentNodeIndex(rs.getInt("node_index"));
        context.setStatus(WorkflowStatus.valueOf(rs.getString("status")));
        context.setSuspendReason(rs.getString("suspend_reason"));
        context.setWaitingEventType(rs.getString("waiting_event_type"));
        context.setFailureMessage(rs.getString("failure_message"));
        context.setCreatedTime(rs.getLong("created_time"));
        context.setUpdatedTime(rs.getLong("updated_time"));

        String varsJson = rs.getString("variables");
        if (varsJson != null && !varsJson.isBlank()) {
            Builder varsBuilder = new Builder();
            varsBuilder.parse(varsJson);
            context.setVariables(varsBuilder);
        }
        return context;
    }
}
