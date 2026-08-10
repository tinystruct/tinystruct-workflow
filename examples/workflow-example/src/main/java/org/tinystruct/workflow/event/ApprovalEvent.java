package org.tinystruct.workflow.event;

public class ApprovalEvent extends WorkflowEvent<ApprovalPayload> {
    public ApprovalEvent(String executionId, ApprovalPayload payload) {
        super("ApprovalEvent", executionId, payload);
    }
}
