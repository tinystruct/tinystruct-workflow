package org.tinystruct.workflow.correlation;

import org.tinystruct.system.EventDispatcher;
import org.tinystruct.workflow.event.WorkflowEvent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Correlates dispatched {@link WorkflowEvent}s back to suspended workflow executions.
 *
 * <p>When a workflow suspends waiting for a specific event type, it registers a resume
 * callback here keyed by execution ID. When an event of that type is dispatched,
 * the registry routes it to the correct callback and removes it.
 *
 * <p>Each event type is subscribed to {@link EventDispatcher} at most once, regardless
 * of how many executions are waiting for it.
 */
public class EventCorrelationRegistry<T extends WorkflowEvent<?>> {

    private final ConcurrentHashMap<String, Consumer<T>> waiting = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Class<?>, Boolean> subscribedTypes = new ConcurrentHashMap<>();

    /**
     * Registers a resume callback for the given execution ID.
     * The callback is invoked at most once, then removed.
     */
    public void register(String executionId, Consumer<T> callback) {
        waiting.put(executionId, callback);
    }

    /**
     * Removes any pending resume callback for the given execution ID.
     * Safe to call even if no callback is registered.
     */
    public void unregister(String executionId) {
        waiting.remove(executionId);
    }

    /**
     * Ensures the given event type is subscribed in {@link EventDispatcher}.
     * Subsequent calls for the same type are no-ops.
     */
    public <E extends T> void registerEventType(Class<E> eventType) {
        if (subscribedTypes.putIfAbsent(eventType, Boolean.TRUE) == null) {
            EventDispatcher.getInstance().registerHandler(eventType, event -> {
                Consumer<T> callback = waiting.remove(event.getExecutionId());
                if (callback != null) {
                    callback.accept(event);
                }
            });
        }
    }
}
