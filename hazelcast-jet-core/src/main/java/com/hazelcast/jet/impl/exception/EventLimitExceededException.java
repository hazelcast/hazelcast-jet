package com.hazelcast.jet.impl.exception;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.TerminationMode;

import javax.annotation.Nonnull;

public class EventLimitExceededException extends JetException {
    public EventLimitExceededException() {
        this(TerminationMode.CANCEL_GRACEFUL);
    }

    private EventLimitExceededException(@Nonnull TerminationMode mode) {
        super(mode.name());
    }
}
