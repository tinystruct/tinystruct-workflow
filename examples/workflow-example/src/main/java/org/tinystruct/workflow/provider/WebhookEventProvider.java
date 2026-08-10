package org.tinystruct.workflow.provider;

import org.tinystruct.AbstractApplication;
import org.tinystruct.ApplicationException;
import org.tinystruct.data.component.Builder;
import org.tinystruct.system.Event;
import org.tinystruct.system.EventDispatcher;
import org.tinystruct.system.annotation.Action;
import org.tinystruct.workflow.event.WebhookEvent;
import org.tinystruct.workflow.event.EventProvider;

public class WebhookEventProvider extends AbstractApplication implements EventProvider {
    private boolean started = false;

    @Override
    public void init() {
        setTemplateRequired(false);
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public boolean supports(Class<? extends Event<?>> eventType) {
        return WebhookEvent.class.isAssignableFrom(eventType);
    }

    @Override
    public void start() {
        this.started = true;
    }

    @Override
    public void stop() {
        this.started = false;
    }

    @Action(value = "workflow/webhook", mode = Action.Mode.HTTP_POST)
    public String handleWebhook(String executionId, String payloadJson) throws ApplicationException {
        if (!started) {
            throw new ApplicationException("WebhookEventProvider is not started");
        }

        Builder payload = new Builder();
        if (payloadJson != null && !payloadJson.trim().isEmpty()) {
            payload.parse(payloadJson);
        }

        WebhookEvent event = new WebhookEvent(executionId, payload);
        EventDispatcher.getInstance().dispatch(event);
        return "{\"status\":\"success\"}";
    }
}
