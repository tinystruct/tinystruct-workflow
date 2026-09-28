package org.tinystruct.workflow.repository;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.tinystruct.workflow.ExecutionContext;
import org.tinystruct.workflow.SnapshotIOException;
import org.tinystruct.workflow.WorkflowStatus;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link SnapshotRepository#compareAndSetStatus} and {@link SnapshotRepository#findByStatus} for the
 * implementations that can be exercised without a server: memory and file. The Redis and JDBC ones
 * push the same two operations down to {@code WATCH}/{@code MULTI} and a conditional {@code UPDATE}.
 */
class SnapshotRepositoryClaimTest {

    private static ExecutionContext waiting(String id) {
        ExecutionContext context = new ExecutionContext();
        context.setExecutionId(id);
        context.setWorkflowId("semantic-confirm");
        context.setStatus(WorkflowStatus.WAITING);
        return context;
    }

    private static ExecutionContext in(WorkflowStatus status, String id) {
        ExecutionContext context = waiting(id);
        context.setStatus(status);
        return context;
    }

    // ---- compareAndSetStatus ----------------------------------------------------------------

    @Test
    void memoryClaimsOnce() throws Exception {
        claimsExactlyOnce(new MemorySnapshotRepository());
    }

    @Test
    void fileClaimsOnce(@TempDir Path dir) throws Exception {
        claimsExactlyOnce(new FileSnapshotRepository(dir));
    }

    private void claimsExactlyOnce(SnapshotRepository repository) throws Exception {
        String id = UUID.randomUUID().toString();
        repository.save(waiting(id));

        assertTrue(repository.compareAndSetStatus(id, WorkflowStatus.WAITING, WorkflowStatus.RUNNING));
        assertEquals(WorkflowStatus.RUNNING, repository.load(id).getStatus());

        assertFalse(repository.compareAndSetStatus(id, WorkflowStatus.WAITING, WorkflowStatus.RUNNING),
                "the second caller must lose");
        assertEquals(WorkflowStatus.RUNNING, repository.load(id).getStatus());
    }

    @Test
    void memoryLetsExactlyOneOfManyThreadsClaim() throws Exception {
        onlyOneThreadWins(new MemorySnapshotRepository());
    }

    @Test
    void fileLetsExactlyOneOfManyThreadsClaim(@TempDir Path dir) throws Exception {
        onlyOneThreadWins(new FileSnapshotRepository(dir));
    }

    private void onlyOneThreadWins(SnapshotRepository repository) throws Exception {
        String id = UUID.randomUUID().toString();
        repository.save(waiting(id));

        int racers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(racers);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger won = new AtomicInteger();
        try {
            List<Callable<Void>> tasks = new java.util.ArrayList<>();
            for (int i = 0; i < racers; i++) {
                tasks.add(() -> {
                    go.await();
                    if (repository.compareAndSetStatus(id, WorkflowStatus.WAITING, WorkflowStatus.RUNNING)) {
                        won.incrementAndGet();
                    }
                    return null;
                });
            }
            List<Future<Void>> futures = new java.util.ArrayList<>();
            for (Callable<Void> task : tasks) futures.add(pool.submit(task));
            go.countDown();
            for (Future<Void> f : futures) f.get(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, won.get(), "exactly one claim may succeed");
    }

    @Test
    void aMissingOrWrongStatusIsNotAClaim() throws Exception {
        SnapshotRepository repository = new MemorySnapshotRepository();
        assertFalse(repository.compareAndSetStatus("no-such-id", WorkflowStatus.WAITING, WorkflowStatus.RUNNING));

        String id = UUID.randomUUID().toString();
        repository.save(in(WorkflowStatus.COMPLETED, id));
        assertFalse(repository.compareAndSetStatus(id, WorkflowStatus.WAITING, WorkflowStatus.RUNNING));
        assertEquals(WorkflowStatus.COMPLETED, repository.load(id).getStatus());
    }

    @Test
    void nullsAreRefused() {
        SnapshotRepository repository = new MemorySnapshotRepository();
        assertThrows(SnapshotIOException.class,
                () -> repository.compareAndSetStatus(null, WorkflowStatus.WAITING, WorkflowStatus.RUNNING));
        assertThrows(SnapshotIOException.class,
                () -> repository.compareAndSetStatus("x", null, WorkflowStatus.RUNNING));
        assertThrows(SnapshotIOException.class, () -> repository.findByStatus(null));
    }

    /** The default is documented as non-atomic, but it must still get the single-caller case right. */
    @Test
    void theDefaultStillTransitions() throws Exception {
        MemorySnapshotRepository backing = new MemorySnapshotRepository();
        SnapshotRepository withDefaults = new SnapshotRepository() {
            @Override public void save(ExecutionContext c) throws SnapshotIOException { backing.save(c); }
            @Override public ExecutionContext load(String id) throws SnapshotIOException { return backing.load(id); }
            @Override public void delete(String id) throws SnapshotIOException { backing.delete(id); }
        };
        String id = UUID.randomUUID().toString();
        withDefaults.save(waiting(id));

        assertTrue(withDefaults.compareAndSetStatus(id, WorkflowStatus.WAITING, WorkflowStatus.RUNNING));
        assertFalse(withDefaults.compareAndSetStatus(id, WorkflowStatus.WAITING, WorkflowStatus.RUNNING));
        assertThrows(UnsupportedOperationException.class, () -> withDefaults.findByStatus(WorkflowStatus.WAITING));
    }

    // ---- findByStatus -----------------------------------------------------------------------

    @Test
    void memoryFindsByStatus() throws Exception {
        findsOnlyTheGivenStatus(new MemorySnapshotRepository());
    }

    @Test
    void fileFindsByStatus(@TempDir Path dir) throws Exception {
        findsOnlyTheGivenStatus(new FileSnapshotRepository(dir));
    }

    private void findsOnlyTheGivenStatus(SnapshotRepository repository) throws Exception {
        repository.save(in(WorkflowStatus.WAITING, "a"));
        repository.save(in(WorkflowStatus.WAITING, "b"));
        repository.save(in(WorkflowStatus.COMPLETED, "c"));
        repository.save(in(WorkflowStatus.CANCELLED, "d"));

        List<String> waiting = repository.findByStatus(WorkflowStatus.WAITING).stream()
                .map(ExecutionContext::getExecutionId).sorted().collect(Collectors.toList());

        assertEquals(List.of("a", "b"), waiting);
        assertEquals(1, repository.findByStatus(WorkflowStatus.COMPLETED).size());
        assertTrue(repository.findByStatus(WorkflowStatus.FAILED).isEmpty());
    }

    @Test
    void anEmptyRepositoryFindsNothing(@TempDir Path dir) throws Exception {
        assertTrue(new FileSnapshotRepository(dir).findByStatus(WorkflowStatus.WAITING).isEmpty());
        assertTrue(new MemorySnapshotRepository().findByStatus(WorkflowStatus.WAITING).isEmpty());
    }

    /** The lock files a claim leaves behind must not be mistaken for snapshots. */
    @Test
    void fileIgnoresItsOwnLockFiles(@TempDir Path dir) throws Exception {
        FileSnapshotRepository repository = new FileSnapshotRepository(dir);
        repository.save(waiting("a"));
        repository.compareAndSetStatus("a", WorkflowStatus.WAITING, WorkflowStatus.RUNNING);

        assertEquals(1, repository.findByStatus(WorkflowStatus.RUNNING).size());
        assertTrue(repository.findByStatus(WorkflowStatus.WAITING).isEmpty());
    }

    @Test
    void deletingRemovesTheSnapshotAndItsLockFile(@TempDir Path dir) throws Exception {
        FileSnapshotRepository repository = new FileSnapshotRepository(dir);
        repository.save(waiting("a"));
        repository.compareAndSetStatus("a", WorkflowStatus.WAITING, WorkflowStatus.RUNNING);

        repository.delete("a");

        assertNull(repository.load("a"));
        try (var entries = java.nio.file.Files.list(dir)) {
            assertEquals(0, entries.count(), "no lock file may be left behind");
        }
    }
}
