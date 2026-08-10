package org.tinystruct.workflow.event;

public class LLMResponseEvent extends WorkflowEvent<String> {
    public LLMResponseEvent(String executionId, String payload) {
        super("LLMResponseEvent", executionId, payload);
    }
}
