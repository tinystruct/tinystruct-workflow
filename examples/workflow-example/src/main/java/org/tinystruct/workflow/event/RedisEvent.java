package org.tinystruct.workflow.event;

public class RedisEvent extends WorkflowEvent<String> {
    public RedisEvent(String executionId, String payload) {
        super("RedisEvent", executionId, payload);
    }
}
