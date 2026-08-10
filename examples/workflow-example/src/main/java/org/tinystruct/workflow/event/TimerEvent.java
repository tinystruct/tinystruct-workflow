package org.tinystruct.workflow.event;

public class TimerEvent extends WorkflowEvent<Void> {
    public TimerEvent(String executionId) {
        super("TimerEvent", executionId, null);
    }
}
