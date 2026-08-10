package org.tinystruct.workflow;

import org.tinystruct.data.component.Builder;
import org.tinystruct.system.Event;
import org.tinystruct.workflow.correlation.EventCorrelationRegistry;
import org.tinystruct.workflow.event.WorkflowEvent;
import org.tinystruct.workflow.repository.SnapshotRepository;

import java.util.Map;
import java.util.UUID;

/**
 * Entry point for the workflow engine.
 *
 * <p>Typical usage:
 * <pre>
 * WorkflowEngine engine = new WorkflowEngine(new MemorySnapshotRepository());
 *
 * engine.registerWorkflow(new WorkflowDefinition("checkout")
 *         .addNode("validate")
 *         .addNode("charge")
 *         .addNode("notify"));
 *
 * String executionId = engine.start("checkout");
 * </pre>
 *
 * <p>To resume a suspended execution manually:
 * <pre>
 * engine.resume(executionId);
 * </pre>
 *
 * <p>Event-driven resume happens automatically when an appropriately typed
 * {@link WorkflowEvent} is dispatched via
 * {@link org.tinystruct.system.EventDispatcher#dispatch(Event)}.
 */
public class WorkflowEngine {

    private final ExecutionRuntime runtime;
    private final SnapshotRepository snapshotRepository;
    private final EventCorrelationRegistry<WorkflowEvent<?>> correlationRegistry;

    public WorkflowEngine(SnapshotRepository snapshotRepository) {
        this.snapshotRepository = snapshotRepository;
        this.correlationRegistry = new EventCorrelationRegistry<>();
        this.runtime = new ExecutionRuntime(snapshotRepository, this.correlationRegistry);
    }

    /**
     * Registers a workflow definition. Must be called before {@link #start(String)}.
     * Re-registering with the same ID replaces the existing definition.
     */
    public void registerWorkflow(WorkflowDefinition definition) {
        runtime.registerWorkflow(definition);
    }

    /**
     * Starts a new execution of the named workflow.
     *
     * @param workflowId the ID passed to {@link WorkflowDefinition#WorkflowDefinition(String)}
     * @return the execution ID, which can be used to query or resume this execution
     * @throws WorkflowNotFoundException if no workflow with that ID has been registered
     * @throws WorkflowException         if the execution fails to start
     */
    public String start(String workflowId) throws WorkflowException {
        return start(workflowId, null);
    }

    /**
     * Starts a new execution of the named workflow with initial variables.
     *
     * @param workflowId the workflow to run
     * @param variables  initial key/value pairs available to the first node, or {@code null}
     * @return the execution ID
     */
    public String start(String workflowId, Map<String, Object> variables) throws WorkflowException {
        if (runtime.getWorkflowDefinition(workflowId) == null) {
            throw new WorkflowNotFoundException(workflowId);
        }

        ExecutionContext context = new ExecutionContext();
        context.setWorkflowId(workflowId);
        context.setExecutionId(UUID.randomUUID().toString());
        context.setStatus(WorkflowStatus.NEW);

        if (variables != null) {
            Builder builder = new Builder();
            variables.forEach(builder::put);
            context.setVariables(builder);
        }

        snapshotRepository.save(context);
        runtime.execute(context);
        return context.getExecutionId();
    }

    /**
     * Manually resumes a WAITING execution with no event payload.
     *
     * @param executionId the execution to resume
     * @throws WorkflowException if the execution is not found or not in WAITING state
     */
    public void resume(String executionId) throws WorkflowException {
        runtime.resume(executionId, null);
    }

    /**
     * Manually resumes a WAITING execution, supplying an event whose payload
     * will be available via {@link Workflow#getVariable(String) Workflow.getVariable("__resumePayload")}.
     *
     * @param executionId the execution to resume
     * @param event       the triggering event, or {@code null}
     */
    public void resume(String executionId, Event<?> event) throws WorkflowException {
        runtime.resume(executionId, event);
    }

    /**
     * Cancels a WAITING execution. Has no effect on already-completed or failed executions.
     *
     * @param executionId the execution to cancel
     * @throws WorkflowNotFoundException if the execution does not exist
     */
    public void cancel(String executionId) throws WorkflowException {
        ExecutionContext context = snapshotRepository.load(executionId);
        if (context == null) {
            throw new WorkflowNotFoundException(executionId);
        }
        context.setStatus(WorkflowStatus.CANCELLED);
        context.setUpdatedTime(System.currentTimeMillis());
        snapshotRepository.save(context);
        correlationRegistry.unregister(executionId);
    }

    /**
     * Returns the current state of an execution.
     *
     * @param executionId the execution to look up
     * @return a snapshot of the execution context
     * @throws WorkflowNotFoundException if the execution does not exist
     */
    public ExecutionContext getExecution(String executionId) throws WorkflowException {
        ExecutionContext context = snapshotRepository.load(executionId);
        if (context == null) {
            throw new WorkflowNotFoundException(executionId);
        }
        return context;
    }
}
