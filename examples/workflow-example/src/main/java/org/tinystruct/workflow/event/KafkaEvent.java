package org.tinystruct.workflow.event;

public class KafkaEvent extends WorkflowEvent<byte[]> {
    public KafkaEvent(String executionId, byte[] payload) {
        super("KafkaEvent", executionId, payload);
    }
}
