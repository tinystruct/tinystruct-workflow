package org.tinystruct.workflow.provider;

import org.tinystruct.system.Event;
import org.tinystruct.system.EventDispatcher;
import org.tinystruct.system.scheduling.Scheduler;
import org.tinystruct.system.scheduling.SchedulerTask;
import org.tinystruct.system.scheduling.TimeIterator;
import org.tinystruct.workflow.event.TimerEvent;
import org.tinystruct.workflow.event.EventProvider;

import java.util.Calendar;

public class TimerEventProvider implements EventProvider {
    private boolean started = false;

    @Override
    public boolean supports(Class<? extends Event<?>> eventType) {
        return TimerEvent.class.isAssignableFrom(eventType);
    }

    @Override
    public void start() {
        this.started = true;
    }

    @Override
    public void stop() {
        this.started = false;
    }

    public void scheduleTimer(String executionId, long delayMillis) {
        if (!started) {
            throw new IllegalStateException("TimerEventProvider is not started");
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis() + delayMillis);
        
        SchedulerTask task = new SchedulerTask() {
            private boolean runned = false;

            @Override
            public void start() {
                if (!runned) {
                    runned = true;
                    TimerEvent event = new TimerEvent(executionId);
                    EventDispatcher.getInstance().dispatch(event);
                }
            }

            @Override
            public boolean next() {
                return false; // one-shot
            }

            @Override
            public void cancel() {
                runned = true;
            }
        };

        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);
        
        Scheduler.getInstance().schedule(task, new TimeIterator(hour, minute, second, calendar.getTime()));
    }
}
