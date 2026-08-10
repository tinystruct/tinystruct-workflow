package org.tinystruct.workflow.event;

import org.tinystruct.data.component.Builder;

public class WebhookEvent extends WorkflowEvent<Builder> {
    public WebhookEvent(String executionId, Builder payload) {
        super("WebhookEvent", executionId, payload);
    }
}
