package org.tinystruct.workflow.event;

public class UserInputEvent extends WorkflowEvent<String> {
    public UserInputEvent(String executionId, String payload) {
        super("UserInputEvent", executionId, payload);
    }
}
