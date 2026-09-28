package org.tinystruct.workflow.repository;

import org.tinystruct.workflow.ExecutionContext;
import org.tinystruct.workflow.SnapshotIOException;
import org.tinystruct.workflow.WorkflowStatus;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * File-system {@link SnapshotRepository} that stores each execution as a JSON file
 * named {@code <executionId>.json} under a configurable directory.
 *
 * <p>Suitable for single-node deployments that need persistence across restarts
 * without a database dependency.
 */
public class FileSnapshotRepository implements SnapshotRepository {

    private final Path directory;

    /** Creates a repository using the {@code snapshots/} directory relative to the working directory. */
    public FileSnapshotRepository() {
        this(Paths.get("snapshots"));
    }

    public FileSnapshotRepository(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create snapshots directory: " + directory, e);
        }
    }

    @Override
    public void save(ExecutionContext context) throws SnapshotIOException {
        if (context == null || context.getExecutionId() == null) {
            throw new SnapshotIOException("Cannot save null context or context with null execution ID");
        }
        Path file = resolve(context.getExecutionId());
        try {
            Files.writeString(file, context.toJson(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SnapshotIOException("Failed to write snapshot: " + file, e);
        }
    }

    @Override
    public ExecutionContext load(String executionId) throws SnapshotIOException {
        if (executionId == null) {
            throw new SnapshotIOException("executionId must not be null");
        }
        Path file = resolve(executionId);
        if (!Files.exists(file)) {
            return null;
        }
        try {
            return ExecutionContext.fromJson(Files.readString(file, StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to read snapshot: " + file, e);
        }
    }

    @Override
    public void delete(String executionId) throws SnapshotIOException {
        if (executionId == null) {
            throw new SnapshotIOException("executionId must not be null");
        }
        try {
            Files.deleteIfExists(resolve(executionId));
            Files.deleteIfExists(lockFile(executionId));
        } catch (IOException e) {
            throw new SnapshotIOException("Failed to delete snapshot for execution: " + executionId, e);
        }
    }

    /**
     * Read-check-write under an exclusive {@link FileLock} on a per-execution lock file, so that
     * two processes on this host cannot both win. {@code synchronized} covers the threads of this
     * JVM, because a file lock is held by the whole JVM and would not exclude them.
     *
     * <p>Across hosts this is only as good as the shared filesystem's locking, which is why a
     * multi-host deployment belongs on {@code redis} or {@code database} instead.
     */
    @Override
    public synchronized boolean compareAndSetStatus(String executionId, WorkflowStatus expected, WorkflowStatus target)
            throws SnapshotIOException {
        if (executionId == null || expected == null || target == null) {
            throw new SnapshotIOException("executionId, expected and target must not be null");
        }
        Path lock = lockFile(executionId);
        try (FileChannel channel = FileChannel.open(lock,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock held = channel.lock()) {
            ExecutionContext context = load(executionId);
            if (context == null || context.getStatus() != expected) {
                return false;
            }
            context.setStatus(target);
            context.setUpdatedTime(System.currentTimeMillis());
            save(context);
            return true;
        } catch (SnapshotIOException e) {
            throw e;
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
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : files.filter(p -> p.getFileName().toString().endsWith(".json")).toList()) {
                ExecutionContext context = read(file);
                if (context != null && context.getStatus() == status) {
                    found.add(context);
                }
            }
        } catch (IOException e) {
            throw new SnapshotIOException("Failed to list snapshots in: " + directory, e);
        }
        return found;
    }

    /** A snapshot that vanished or is half-written while we walk the directory is skipped, not fatal. */
    private static ExecutionContext read(Path file) throws SnapshotIOException {
        try {
            return ExecutionContext.fromJson(Files.readString(file, StandardCharsets.UTF_8));
        } catch (java.nio.file.NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw new SnapshotIOException("Failed to read snapshot: " + file, e);
        } catch (Exception e) {
            return null;
        }
    }

    private Path resolve(String executionId) {
        return directory.resolve(executionId + ".json");
    }

    private Path lockFile(String executionId) {
        return directory.resolve(executionId + ".lock");
    }
}
