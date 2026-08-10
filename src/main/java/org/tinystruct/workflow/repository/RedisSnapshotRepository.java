package org.tinystruct.workflow.repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.tinystruct.system.Configuration;
import org.tinystruct.system.Settings;
import org.tinystruct.workflow.ExecutionContext;
import org.tinystruct.workflow.SnapshotIOException;

import java.io.Closeable;
import java.time.Duration;

/**
 * Redis-backed {@link SnapshotRepository} using Lettuce.
 *
 * <p>Reads connection settings from {@link Settings}:
 * <ul>
 *   <li>{@code redis.host} — defaults to {@code localhost}</li>
 *   <li>{@code redis.port} — defaults to {@code 6379}</li>
 *   <li>{@code redis.password} — optional</li>
 * </ul>
 *
 * <p>Implements {@link Closeable}; call {@link #close()} when the engine shuts down.
 */
public class RedisSnapshotRepository implements SnapshotRepository, Closeable {

    private static final String KEY_PREFIX = "workflow:snapshot:";
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;

    public RedisSnapshotRepository() {
        Configuration<String> config = new Settings();
        String host     = nonEmpty(config.get("redis.host"),     "localhost");
        int    port     = Integer.parseInt(nonEmpty(config.get("redis.port"), "6379"));
        String password = config.get("redis.password");

        RedisURI.Builder uriBuilder = RedisURI.Builder.redis(host, port)
                .withDatabase(0)
                .withTimeout(TIMEOUT);
        if (password != null && !password.isBlank()) {
            uriBuilder.withPassword(password.toCharArray());
        }

        this.client     = RedisClient.create(uriBuilder.build());
        this.connection = client.connect();
        this.commands   = connection.sync();
    }

    @Override
    public void save(ExecutionContext context) throws SnapshotIOException {
        if (context == null || context.getExecutionId() == null) {
            throw new SnapshotIOException("Cannot save null context or context with null execution ID");
        }
        String key = key(context.getExecutionId());
        try {
            commands.set(key, context.toJson());
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to save snapshot to Redis for key: " + key, e);
        }
    }

    @Override
    public ExecutionContext load(String executionId) throws SnapshotIOException {
        if (executionId == null) {
            throw new SnapshotIOException("executionId must not be null");
        }
        String key = key(executionId);
        try {
            String json = commands.get(key);
            return json != null ? ExecutionContext.fromJson(json) : null;
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to load snapshot from Redis for key: " + key, e);
        }
    }

    @Override
    public void delete(String executionId) throws SnapshotIOException {
        if (executionId == null) {
            throw new SnapshotIOException("executionId must not be null");
        }
        String key = key(executionId);
        try {
            commands.del(key);
        } catch (Exception e) {
            throw new SnapshotIOException("Failed to delete snapshot from Redis for key: " + key, e);
        }
    }

    @Override
    public void close() {
        try { connection.close(); } catch (Exception ignored) {}
        try { client.shutdown();  } catch (Exception ignored) {}
    }

    private String key(String executionId) {
        return KEY_PREFIX + executionId;
    }

    private static String nonEmpty(String value, String fallback) {
        return (value != null && !value.isBlank()) ? value : fallback;
    }
}
