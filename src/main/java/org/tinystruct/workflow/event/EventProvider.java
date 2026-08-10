package org.tinystruct.workflow.event;

import org.tinystruct.system.Event;

public interface EventProvider {
    boolean supports(Class<? extends Event<?>> eventType);
    void start();
    void stop();
}
