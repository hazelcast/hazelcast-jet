/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.JetException;

/**
 * Thrown when a named job is submitted while there is an active job,
 * i.e., not completed/failed job, in the system with the same name.
 */
public class DuplicateActiveJobNameException extends JetException {

    /**
     * Creates the exception
     */
    public DuplicateActiveJobNameException() {
    }

    /**
     * Creates the exception with a message.
     */
    public DuplicateActiveJobNameException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a message and a cause.
     */
    public DuplicateActiveJobNameException(String message, Throwable cause) {
        super(message, cause);
    }
}
