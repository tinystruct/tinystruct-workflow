package org.tinystruct.workflow.repository;

import org.tinystruct.workflow.ExecutionContext;
import org.tinystruct.workflow.SnapshotIOException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
        } catch (IOException e) {
            throw new SnapshotIOException("Failed to delete snapshot for execution: " + executionId, e);
        }
    }

    private Path resolve(String executionId) {
        return directory.resolve(executionId + ".json");
    }
}
