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

package com.hazelcast.jet.impl.util;

import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.LogEvent;

import java.util.logging.Level;

public final class DisabledLogger extends AbstractLogger {

    public static final DisabledLogger INSTANCE = new DisabledLogger();

    private DisabledLogger() {
    }

    @Override
    public void log(Level level, String message) {
    }

    @Override
    public void log(Level level, String message, Throwable thrown) {
    }

    @Override
    public void log(LogEvent logEvent) {
    }

    @Override
    public Level getLevel() {
        return Level.OFF;
    }

    @Override
    public boolean isLoggable(Level level) {
        return false;
    }
}
