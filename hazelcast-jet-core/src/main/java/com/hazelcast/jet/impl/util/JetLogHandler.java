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

import com.hazelcast.instance.BuildInfoProvider;

import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

public class JetLogHandler extends StreamHandler {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    public JetLogHandler() {
        super(System.out, new JetLogFormatter());
    }

    @Override
    public void publish(LogRecord record) {
        // sync method call, we should implement own logger here.
        super.publish(record);
        flush();
    }

    private static class JetLogFormatter extends Formatter {

        /**
         * Temporary, remove after https://github.com/hazelcast/hazelcast/pull/16622 is merged
         * Currently the filtering doesn't work for client, because it uses IMDG version.
         */
        private static final String VERSION_STR =
                "[" + BuildInfoProvider.getBuildInfo().getJetBuildInfo().getVersion() + "]";

        private static final boolean ENABLE_DETAILS = Boolean.parseBoolean(
                getDetailsProperty());

        private static String getDetailsProperty() {
            return System.getProperties().getProperty("hazelcast.logging.details.enabled", "false");
        }

        @Override
        public String format(LogRecord record) {
            String loggerName = record.getLoggerName();
            int idx = loggerName.lastIndexOf((int) '.');
            if (idx > 0 && loggerName.length() > idx + 1) {
                loggerName = loggerName.substring(idx + 1);
            }
            String message = record.getMessage();
            if (!ENABLE_DETAILS) {
                int versionIdx = message.indexOf(VERSION_STR);
                if (versionIdx > 0) {
                    message = message.substring(versionIdx + VERSION_STR.length() + 1);
                }
            }
            return String.format("%s [%s%7s%s] [%s%25s%s] %s%n",
                    Util.toLocalTime(record.getMillis()),
                    getColor(record.getLevel()),
                    record.getLevel(),
                    ANSI_RESET,
                    ANSI_BLUE,
                    loggerName,
                    ANSI_RESET,
                    message);
        }

        private String getColor(Level level) {
            switch (level.intValue()) {
                case 1000: // ERROR
                    return ANSI_RED;
                case 900: // WARN
                    return ANSI_YELLOW;
                case 800: // INFO
                    return ANSI_GREEN;
                default:
                    return ANSI_RESET;
            }
        }
    }
}
