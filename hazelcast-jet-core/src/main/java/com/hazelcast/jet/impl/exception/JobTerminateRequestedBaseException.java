/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.exception;

import com.hazelcast.jet.JetException;

public class JobTerminateRequestedBaseException extends JetException {
    private final boolean withTerminalSnapshot;

    JobTerminateRequestedBaseException(boolean withTerminalSnapshot) {
        this.withTerminalSnapshot = withTerminalSnapshot;
    }

    public boolean isWithTerminalSnapshot() {
        return withTerminalSnapshot;
    }
}
