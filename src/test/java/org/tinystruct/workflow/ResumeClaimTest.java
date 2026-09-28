package org.tinystruct.workflow;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.tinystruct.AbstractApplication;
import org.tinystruct.system.ApplicationManager;
import org.tinystruct.system.Settings;
import org.tinystruct.system.annotation.Action;
import org.tinystruct.workflow.repository.MemorySnapshotRepository;
import org.tinystruct.workflow.repository.SnapshotRepository;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * How {@link WorkflowEngine#resume} claims an execution.
 *
 * <p>The guarantee has to come from the repository, because {@code DistributedLock} coordinates
 * through a lock file in the working directory — two hosts sharing Redis or a database do not share
 * that file, so the lock alone would let both of them resume the same execution.
 */
class ResumeClaimTest {

    private static final AtomicInteger RUNS = new AtomicInteger();

    public static class Nodes extends AbstractApplication {
        @Override public void init() { setTemplateRequired(false); }
        @Override public String version() { return "1.0"; }

        @Action("claim-test/await")
        public void await() {
            Workflow.suspend("waiting");
        }

        @Action("claim-test/act")
        public void act() {
            RUNS.incrementAndGet();
        }
    }

    @BeforeAll
    static void install() {
        ApplicationManager.install(new Nodes(), new Settings());
    }

    private static WorkflowEngine engineOver(SnapshotRepository repository) {
        WorkflowEngine engine = new WorkflowEngine(repository);
        engine.registerWorkflow(new WorkflowDefinition("claim-test")
                .addNode("claim-test/await")
                .addNode("claim-test/act"));
        return engine;
    }

    @Test
    void anExecutionAnotherClaimantTookCannotBeResumed() throws Exception {
        RUNS.set(0);
        SnapshotRepository repository = new MemorySnapshotRepository();
        WorkflowEngine engine = engineOver(repository);
        String id = engine.start("claim-test");
        assertEquals(WorkflowStatus.WAITING, repository.load(id).getStatus());

        // What another host winning the race looks like to this one.
        assertTrue(repository.compareAndSetStatus(id, WorkflowStatus.WAITING, WorkflowStatus.RUNNING));

        WorkflowException e = assertThrows(WorkflowException.class, () -> engine.resume(id));
        assertTrue(e.getMessage().contains("RUNNING"), e.getMessage());
        assertEquals(0, RUNS.get());
    }

    @Test
    void resumingClaimsTheExecutionAndRunsTheRemainingNodes() throws Exception {
        RUNS.set(0);
        SnapshotRepository repository = new MemorySnapshotRepository();
        WorkflowEngine engine = engineOver(repository);
        String id = engine.start("claim-test");

        engine.resume(id);

        assertEquals(1, RUNS.get());
        assertEquals(WorkflowStatus.COMPLETED, repository.load(id).getStatus());
    }

    @Test
    void aSecondResumeAfterCompletionIsRefused() throws Exception {
        RUNS.set(0);
        SnapshotRepository repository = new MemorySnapshotRepository();
        WorkflowEngine engine = engineOver(repository);
        String id = engine.start("claim-test");
        engine.resume(id);

        assertThrows(WorkflowException.class, () -> engine.resume(id));
        assertEquals(1, RUNS.get(), "the action runs exactly once");
    }

    /**
     * A claim that leads nowhere has to be handed back, or the execution is stranded RUNNING and
     * nobody can ever resume it again. An engine that never registered the definition is the
     * simplest way to fail after the claim but before any node runs.
     */
    @Test
    void aResumeThatFailsBeforeAnyNodeRunsHandsTheClaimBack() throws Exception {
        RUNS.set(0);
        SnapshotRepository repository = new MemorySnapshotRepository();
        String id = engineOver(repository).start("claim-test");

        WorkflowEngine withoutDefinition = new WorkflowEngine(repository);
        assertThrows(WorkflowException.class, () -> withoutDefinition.resume(id));

        assertEquals(WorkflowStatus.WAITING, repository.load(id).getStatus(),
                "the execution must still be answerable");
        assertEquals(0, RUNS.get());

        // And it really is still answerable, on an engine that knows the workflow.
        engineOver(repository).resume(id);
        assertEquals(1, RUNS.get());
    }

    @Test
    void aNodeThatFailsLeavesTheExecutionFailedNotStranded() throws Exception {
        SnapshotRepository repository = new MemorySnapshotRepository();
        WorkflowEngine engine = new WorkflowEngine(repository);
        engine.registerWorkflow(new WorkflowDefinition("claim-test-boom")
                .addNode("claim-test/await")
                .addNode("no-such-action"));
        String id = engine.start("claim-test-boom");

        assertThrows(WorkflowException.class, () -> engine.resume(id));

        assertEquals(WorkflowStatus.FAILED, repository.load(id).getStatus());
    }

    @Test
    void resumingSomethingThatIsNotThereIsNotFound() {
        WorkflowEngine engine = engineOver(new MemorySnapshotRepository());
        assertThrows(WorkflowNotFoundException.class, () -> engine.resume("no-such-execution"));
    }
}
