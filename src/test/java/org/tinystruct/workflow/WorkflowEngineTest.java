package org.tinystruct.workflow;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.tinystruct.AbstractApplication;
import org.tinystruct.system.ApplicationManager;
import org.tinystruct.system.EventDispatcher;
import org.tinystruct.system.Settings;
import org.tinystruct.system.annotation.Action;
import org.tinystruct.workflow.event.WorkflowEvent;
import org.tinystruct.workflow.repository.MemorySnapshotRepository;
import org.tinystruct.workflow.repository.SnapshotRepository;

import static org.junit.jupiter.api.Assertions.*;

public class WorkflowEngineTest {

    public static class TestEvent extends WorkflowEvent<String> {
        public TestEvent(String executionId, String payload) {
            super("TestEvent", executionId, payload);
        }
    }

    public static class ApprovalEvent extends WorkflowEvent<Boolean> {
        public ApprovalEvent(String executionId, boolean approved) {
            super("ApprovalEvent", executionId, approved);
        }
    }

    public static class TestApplication extends AbstractApplication {
        @Override
        public void init() {
            setTemplateRequired(false);
        }

        @Override
        public String version() {
            return "1.0.0";
        }

        @Action("step1")
        public String step1() {
            Workflow.setVariable("testVar", "hello");
            return "step1";
        }

        @Action("step2")
        public String step2() {
            Workflow.suspend("Waiting for test event", TestEvent.class);
            return "suspended";
        }

        @Action("step3")
        public String step3() {
            Object payload = Workflow.getVariable("__resumePayload");
            return "completed with: " + payload;
        }

        @Action("prepare")
        public String prepare() {
            Workflow.setVariable("task", "deploy-to-production");
            return "prepare";
        }

        @Action("request-approval")
        public String requestApproval() {
            Workflow.suspend("Waiting for human approval", ApprovalEvent.class);
            return "suspended";
        }

        @Action("execute-task")
        public String executeTask() {
            Boolean approved = (Boolean) Workflow.getVariable("__resumePayload");
            if (approved == null || !approved) {
                throw new RuntimeException("Task rejected by approver");
            }
            String task = (String) Workflow.getVariable("task");
            Workflow.setVariable("result", "executed: " + task);
            return "done";
        }
    }

    @BeforeAll
    public static void setup() {
        Settings settings = new Settings();
        TestApplication app = new TestApplication();
        ApplicationManager.install(app, settings);
    }

    @Test
    public void testCoreWorkflowExecution() throws Exception {
        SnapshotRepository repo = new MemorySnapshotRepository();
        WorkflowEngine engine = new WorkflowEngine(repo);

        WorkflowDefinition definition = new WorkflowDefinition("test-wf")
                .addNode("step1")
                .addNode("step2")
                .addNode("step3");

        engine.registerWorkflow(definition);

        String executionId = engine.start("test-wf");
        assertNotNull(executionId);

        ExecutionContext context = engine.getExecution(executionId);
        assertNotNull(context);
        assertEquals(WorkflowStatus.WAITING, context.getStatus());
        assertEquals(1, context.getCurrentNodeIndex());
        assertEquals("hello", context.getVariables().get("testVar").toString());

        // Dispatch test event
        TestEvent event = new TestEvent(executionId, "payload-data");
        EventDispatcher.getInstance().dispatch(event);

        // Verify it completed
        ExecutionContext finalContext = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.COMPLETED, finalContext.getStatus());
        assertEquals(3, finalContext.getCurrentNodeIndex());
    }

    @Test
    public void testApprovalWorkflow_approved() throws Exception {
        SnapshotRepository repo = new MemorySnapshotRepository();
        WorkflowEngine engine = new WorkflowEngine(repo);

        WorkflowDefinition definition = new WorkflowDefinition("approval-wf")
                .addNode("prepare")
                .addNode("request-approval")
                .addNode("execute-task");

        engine.registerWorkflow(definition);

        // Start — runs "prepare", then suspends at "request-approval"
        String executionId = engine.start("approval-wf");
        assertNotNull(executionId);

        ExecutionContext context = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.WAITING, context.getStatus());
        assertEquals(1, context.getCurrentNodeIndex());
        assertEquals("deploy-to-production", context.getVariables().get("task").toString());
        assertEquals(ApprovalEvent.class.getName(), context.getWaitingEventType());

        // Simulate human approval
        ApprovalEvent approval = new ApprovalEvent(executionId, true);
        EventDispatcher.getInstance().dispatch(approval);

        // Verify the task executed and completed
        ExecutionContext finalContext = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.COMPLETED, finalContext.getStatus());
        assertEquals(3, finalContext.getCurrentNodeIndex());
        assertEquals("executed: deploy-to-production", finalContext.getVariables().get("result").toString());
    }

    @Test
    public void testApprovalWorkflow_rejected() throws Exception {
        SnapshotRepository repo = new MemorySnapshotRepository();
        WorkflowEngine engine = new WorkflowEngine(repo);

        WorkflowDefinition definition = new WorkflowDefinition("approval-wf-reject")
                .addNode("prepare")
                .addNode("request-approval")
                .addNode("execute-task");

        engine.registerWorkflow(definition);

        // Start — runs "prepare", then suspends at "request-approval"
        String executionId = engine.start("approval-wf-reject");
        assertNotNull(executionId);

        ExecutionContext context = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.WAITING, context.getStatus());

        // Simulate human rejection — the exception is handled internally by the engine,
        // so dispatch() itself does not throw; we verify the resulting FAILED state instead.
        ApprovalEvent rejection = new ApprovalEvent(executionId, false);
        EventDispatcher.getInstance().dispatch(rejection);

        ExecutionContext finalContext = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.FAILED, finalContext.getStatus());
        assertNotNull(finalContext.getFailureMessage());
        assertTrue(finalContext.getFailureMessage().contains("Task rejected by approver"));
    }
}
