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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.impl.TerminationMode;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

/**
 * Wraps a CompletableFuture that is expected to be completed
 * only in a single way and provides methods for ease of use
 */
public class CompletionToken {
    private final CompletableFuture<TerminationMode> future = new CompletableFuture<>();
    private final ILogger logger;

    public CompletionToken(ILogger logger) {
        this.logger = logger;
    }

    /**
     * @return {@code true} if this invocation caused this CompletionToken to
     * transition to a completed state, else {@code false}.
     */
    public boolean complete(TerminationMode mode) {
        return future.complete(mode);
    }

    /**
     * Returns the value the token was completed with or {@code null}.
     */
    @Nullable
    public TerminationMode get() {
        return future.getNow(null);
    }

    public void whenCompleted(Consumer<TerminationMode> runnable) {
        future.whenComplete(withTryCatch(logger, (result, throwable) -> {
            assert throwable == null : "throwable=" + throwable;
            runnable.accept(result);
        }));
    }
}
