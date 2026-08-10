package org.tinystruct.workflow;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.tinystruct.ApplicationContext;
import org.tinystruct.application.Context;
import org.tinystruct.system.ApplicationManager;
import org.tinystruct.system.EventDispatcher;
import org.tinystruct.system.Settings;
import org.tinystruct.workflow.event.ApprovalEvent;
import org.tinystruct.workflow.event.ApprovalPayload;
import org.tinystruct.workflow.examples.SampleWorkflowApplication;
import org.tinystruct.workflow.provider.TimerEventProvider;
import org.tinystruct.workflow.provider.WebhookEventProvider;
import org.tinystruct.workflow.repository.FileSnapshotRepository;
import org.tinystruct.workflow.repository.MemorySnapshotRepository;
import org.tinystruct.workflow.repository.SnapshotRepository;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class WorkflowEngineTest {

    @BeforeAll
    public static void setup() {
        Settings settings = new Settings();
        SampleWorkflowApplication app = new SampleWorkflowApplication();
        WebhookEventProvider webhookProvider = new WebhookEventProvider();
        
        ApplicationManager.install(app, settings);
        ApplicationManager.install(webhookProvider, settings);
    }

    @Test
    public void testApprovalWorkflow() throws Exception {
        SnapshotRepository repo = new MemorySnapshotRepository();
        WorkflowEngine engine = new WorkflowEngine(repo);

        WorkflowDefinition definition = new WorkflowDefinition("approval-wf")
                .addNode("order/validate")
                .addNode("order/approve")
                .addNode("order/fulfill");

        engine.registerWorkflow(definition);

        String executionId = engine.start("approval-wf");
        assertNotNull(executionId);

        ExecutionContext context = engine.getExecution(executionId);
        assertNotNull(context);
        assertEquals(WorkflowStatus.WAITING, context.getStatus());
        assertEquals(1, context.getCurrentNodeIndex());
        assertEquals("ORD-12345", context.getVariables().get("orderId").toString());

        // Dispatch approval event
        ApprovalPayload payload = new ApprovalPayload(true, "Order approved by manager");
        ApprovalEvent event = new ApprovalEvent(executionId, payload);
        EventDispatcher.getInstance().dispatch(event);

        // Verify it completed
        ExecutionContext finalContext = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.COMPLETED, finalContext.getStatus());
        assertEquals(3, finalContext.getCurrentNodeIndex());
    }

    @Test
    public void testTimerWorkflow() throws Exception {
        SnapshotRepository repo = new MemorySnapshotRepository();
        WorkflowEngine engine = new WorkflowEngine(repo);

        WorkflowDefinition definition = new WorkflowDefinition("timer-wf")
                .addNode("job/start")
                .addNode("job/wait")
                .addNode("job/complete");

        engine.registerWorkflow(definition);

        TimerEventProvider timerProvider = new TimerEventProvider();
        timerProvider.start();

        String executionId = engine.start("timer-wf");
        assertNotNull(executionId);

        ExecutionContext context = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.WAITING, context.getStatus());
        assertEquals(1, context.getCurrentNodeIndex());

        // Schedule timer to fire in 100 milliseconds
        timerProvider.scheduleTimer(executionId, 100);

        // Wait up to 2 seconds for timer to fire and resume workflow
        long start = System.currentTimeMillis();
        ExecutionContext finalContext = null;
        while (System.currentTimeMillis() - start < 2000) {
            finalContext = engine.getExecution(executionId);
            if (finalContext.getStatus() == WorkflowStatus.COMPLETED) {
                break;
            }
            Thread.sleep(50);
        }

        assertNotNull(finalContext);
        assertEquals(WorkflowStatus.COMPLETED, finalContext.getStatus());
        assertEquals(3, finalContext.getCurrentNodeIndex());
        timerProvider.stop();
    }

    @Test
    public void testWebhookWorkflow() throws Exception {
        SnapshotRepository repo = new MemorySnapshotRepository();
        WorkflowEngine engine = new WorkflowEngine(repo);

        WorkflowDefinition definition = new WorkflowDefinition("webhook-wf")
                .addNode("deploy/prepare")
                .addNode("deploy/wait-confirmation")
                .addNode("deploy/finalize");

        engine.registerWorkflow(definition);

        WebhookEventProvider webhookProvider = (WebhookEventProvider) ApplicationManager.get(WebhookEventProvider.class.getName());
        assertNotNull(webhookProvider);
        webhookProvider.start();

        String executionId = engine.start("webhook-wf");
        assertNotNull(executionId);

        ExecutionContext context = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.WAITING, context.getStatus());
        assertEquals(1, context.getCurrentNodeIndex());

        // Call Webhook endpoint directly using ApplicationManager
        Context appCtx = new ApplicationContext();
        appCtx.setAttribute("executionId", executionId);
        appCtx.setAttribute("payloadJson", "{\"confirmed\":true}");
        
        // Simulating the HTTP POST /workflow/webhook path
        ApplicationManager.call("workflow/webhook/" + executionId + "/{\"confirmed\":true}", null, org.tinystruct.system.annotation.Action.Mode.HTTP_POST);

        // Verify workflow completed
        ExecutionContext finalContext = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.COMPLETED, finalContext.getStatus());
        assertEquals(3, finalContext.getCurrentNodeIndex());
        webhookProvider.stop();
    }

    @Test
    public void testFileSnapshotRepositoryRoundTrip() throws Exception {
        SnapshotRepository repo = new FileSnapshotRepository(Paths.get("target/test-snapshots"));
        WorkflowEngine engine = new WorkflowEngine(repo);

        WorkflowDefinition definition = new WorkflowDefinition("file-wf")
                .addNode("order/validate")
                .addNode("order/approve")
                .addNode("order/fulfill");

        engine.registerWorkflow(definition);

        String executionId = engine.start("file-wf");
        assertNotNull(executionId);

        ExecutionContext context = engine.getExecution(executionId);
        assertNotNull(context);
        assertEquals(WorkflowStatus.WAITING, context.getStatus());

        // Dispatch approval event
        ApprovalPayload payload = new ApprovalPayload(true, "Looks good");
        ApprovalEvent event = new ApprovalEvent(executionId, payload);
        EventDispatcher.getInstance().dispatch(event);

        // Verify from file snapshot
        ExecutionContext finalContext = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.COMPLETED, finalContext.getStatus());
    }

    @Test
    public void testConcurrencyLocking() throws Exception {
        SnapshotRepository repo = new MemorySnapshotRepository();
        WorkflowEngine engine = new WorkflowEngine(repo);

        WorkflowDefinition definition = new WorkflowDefinition("concurrent-wf")
                .addNode("order/validate")
                .addNode("order/approve")
                .addNode("order/fulfill");

        engine.registerWorkflow(definition);

        String executionId = engine.start("concurrent-wf");

        // We will trigger resume concurrently using two threads.
        // One thread should succeed; the other should fail or throw because status changes to COMPLETED.
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);
        
        AtomicInteger successCounter = new AtomicInteger(0);
        AtomicInteger failureCounter = new AtomicInteger(0);

        Runnable resumeTask = () -> {
            try {
                startLatch.await();
                ApprovalPayload payload = new ApprovalPayload(true, "approved");
                engine.resume(executionId, new ApprovalEvent(executionId, payload));
                successCounter.incrementAndGet();
            } catch (Exception e) {
                failureCounter.incrementAndGet();
            } finally {
                doneLatch.countDown();
            }
        };

        new Thread(resumeTask).start();
        new Thread(resumeTask).start();

        startLatch.countDown(); // Let them race
        doneLatch.await(3, TimeUnit.SECONDS);

        // One should have succeeded in acquiring lock and resuming, and the other should have failed.
        assertEquals(1, successCounter.get(), "Only one thread should successfully resume the workflow");
        assertEquals(1, failureCounter.get(), "The other thread should fail because workflow status was not WAITING");
    }

    @Test
    public void testCancellation() throws Exception {
        SnapshotRepository repo = new MemorySnapshotRepository();
        WorkflowEngine engine = new WorkflowEngine(repo);

        WorkflowDefinition definition = new WorkflowDefinition("cancel-wf")
                .addNode("order/validate")
                .addNode("order/approve")
                .addNode("order/fulfill");

        engine.registerWorkflow(definition);

        String executionId = engine.start("cancel-wf");
        ExecutionContext context = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.WAITING, context.getStatus());

        engine.cancel(executionId);
        ExecutionContext cancelledContext = engine.getExecution(executionId);
        assertEquals(WorkflowStatus.CANCELLED, cancelledContext.getStatus());

        // Trying to resume a cancelled workflow should throw an exception
        assertThrows(WorkflowException.class, () -> {
            ApprovalPayload payload = new ApprovalPayload(true, "approved");
            engine.resume(executionId, new ApprovalEvent(executionId, payload));
        });
    }
}
