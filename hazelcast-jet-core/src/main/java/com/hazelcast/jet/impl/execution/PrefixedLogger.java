/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.execution;

import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogEvent;

import java.util.logging.Level;
import java.util.logging.LogRecord;

public class PrefixedLogger extends AbstractLogger {

    private static final String PATTERN = "%s -> %s";

    private final ILogger wrapped;
    private final String prefix;

    public PrefixedLogger(ILogger wrapped, String prefix) {
        this.wrapped = wrapped;
        this.prefix = prefix;
    }

    @Override
    public void log(Level level, String message) {
        wrapped.log(level, String.format(PATTERN, prefix, message));
    }

    @Override
    public void log(Level level, String message, Throwable thrown) {
        wrapped.log(level, String.format(PATTERN, prefix, message), thrown);
    }

    @Override
    public void log(LogEvent logEvent) {
        LogRecord logRecord = logEvent.getLogRecord();
        logRecord.setMessage(String.format(PATTERN, prefix, logRecord.getMessage()));
        wrapped.log(logEvent);
    }

    @Override
    public Level getLevel() {
        return wrapped.getLevel();
    }

    @Override
    public boolean isLoggable(Level level) {
        return wrapped.isLoggable(level);
    }
}
