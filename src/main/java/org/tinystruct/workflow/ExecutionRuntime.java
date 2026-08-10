package org.tinystruct.workflow;

import org.tinystruct.ApplicationContext;
import org.tinystruct.application.Context;
import org.tinystruct.system.ApplicationManager;
import org.tinystruct.system.Event;
import org.tinystruct.valve.DistributedLock;
import org.tinystruct.valve.Lock;
import org.tinystruct.workflow.correlation.EventCorrelationRegistry;
import org.tinystruct.workflow.event.WorkflowEvent;
import org.tinystruct.workflow.repository.SnapshotRepository;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Internal execution engine. Drives the node-by-node execution loop, handles
 * suspend/resume, and persists state to the {@link SnapshotRepository}.
 *
 * <p>This class is package-private. External callers interact only through {@link WorkflowEngine}.
 */
class ExecutionRuntime {
    private static final Logger logger = Logger.getLogger(ExecutionRuntime.class.getName());

    private final SnapshotRepository snapshotRepository;
    private final EventCorrelationRegistry<WorkflowEvent<?>> correlationRegistry;
    private final ConcurrentHashMap<String, WorkflowDefinition> definitions = new ConcurrentHashMap<>();

    ExecutionRuntime(SnapshotRepository snapshotRepository,
                     EventCorrelationRegistry<WorkflowEvent<?>> correlationRegistry) {
        this.snapshotRepository = snapshotRepository;
        this.correlationRegistry = correlationRegistry;
    }

    void registerWorkflow(WorkflowDefinition definition) {
        definitions.put(definition.getWorkflowId(), definition);
    }

    WorkflowDefinition getWorkflowDefinition(String workflowId) {
        return definitions.get(workflowId);
    }

    void execute(ExecutionContext context) throws WorkflowException {
        WorkflowDefinition def = definitions.get(context.getWorkflowId());
        if (def == null) {
            throw new WorkflowNotFoundException(context.getWorkflowId());
        }

        while (context.getCurrentNodeIndex() < def.size()) {
            int index = context.getCurrentNodeIndex();
            String actionPath = def.getNode(index);

            context.setStatus(WorkflowStatus.RUNNING);
            context.setUpdatedTime(System.currentTimeMillis());
            Workflow.CURRENT.set(context);

            try {
                Context appCtx = new ApplicationContext();
                appCtx.setId(context.getExecutionId());
                logger.log(Level.FINE, "Executing node [{0}]: {1}", new Object[]{index, actionPath});
                ApplicationManager.call(actionPath, appCtx);
                context.setCurrentNodeIndex(index + 1);

            } catch (Exception e) {
                SuspendExecutionException suspend = findSuspendCause(e);
                if (suspend != null) {
                    handleSuspend(context, index, suspend);
                    return;
                }
                handleFailure(context, actionPath, e);

            } finally {
                Workflow.CURRENT.remove();
            }
        }

        context.setStatus(WorkflowStatus.COMPLETED);
        context.setUpdatedTime(System.currentTimeMillis());
        snapshotRepository.save(context);
        logger.log(Level.INFO, "Workflow completed: {0}", context.getExecutionId());
    }

    void resume(String executionId, Event<?> event) throws WorkflowException {
        Lock lock = new DistributedLock("wf:" + executionId);
        try {
            lock.lock();

            ExecutionContext context = snapshotRepository.load(executionId);
            if (context == null) {
                throw new WorkflowNotFoundException(executionId);
            }
            if (context.getStatus() != WorkflowStatus.WAITING) {
                throw new WorkflowException("Cannot resume execution " + executionId
                        + " — current status: " + context.getStatus());
            }

            if (event != null) {
                context.getVariables().put("__resumePayload", event.getPayload());
            }
            context.setCurrentNodeIndex(context.getCurrentNodeIndex() + 1);
            execute(context);

        } finally {
            lock.unlock();
        }
    }

    // --- private helpers ---

    private void handleSuspend(ExecutionContext context, int nodeIndex,
                                SuspendExecutionException suspend) throws WorkflowException {
        context.setStatus(WorkflowStatus.WAITING);
        context.setSuspendReason(suspend.getReason());

        if (suspend.getWaitingEventType() != null) {
            context.setWaitingEventType(suspend.getWaitingEventType().getName());

            correlationRegistry.register(context.getExecutionId(), event -> {
                try {
                    resume(context.getExecutionId(), event);
                } catch (Exception ex) {
                    logger.log(Level.WARNING, "Workflow execution failed after resume: "
                            + context.getExecutionId() + " — " + ex.getMessage());
                }
            });

            @SuppressWarnings("unchecked")
            Class<? extends WorkflowEvent<?>> eventType =
                    (Class<? extends WorkflowEvent<?>>) suspend.getWaitingEventType();
            correlationRegistry.registerEventType(eventType);
        }

        context.setUpdatedTime(System.currentTimeMillis());
        snapshotRepository.save(context);
        logger.log(Level.INFO, "Workflow suspended at node [{0}]: {1}",
                new Object[]{nodeIndex, suspend.getReason()});
    }

    private void handleFailure(ExecutionContext context, String actionPath,
                                Exception cause) throws WorkflowException {
        context.setStatus(WorkflowStatus.FAILED);
        context.setFailureMessage(cause.getMessage());
        context.setUpdatedTime(System.currentTimeMillis());
        try {
            snapshotRepository.save(context);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Failed to persist FAILED state for execution: "
                    + context.getExecutionId(), ex);
        }
        throw new WorkflowException("Node failed: " + actionPath, cause);
    }

    /** Walks the exception cause chain looking for a {@link SuspendExecutionException}. */
    private SuspendExecutionException findSuspendCause(Throwable t) {
        if (t == null) return null;
        if (t instanceof SuspendExecutionException) return (SuspendExecutionException) t;
        return findSuspendCause(t.getCause());
    }
}
