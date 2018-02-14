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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.JetService;
import com.hazelcast.spi.ExceptionAction;

import static com.hazelcast.jet.impl.util.ExceptionUtil.isTopologicalFailure;
import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;

public abstract class AsyncOperation extends AbstractJobOperation {

    protected AsyncOperation() {
    }

    protected AsyncOperation(long jobId) {
        super(jobId);
    }


    @Override
    public void beforeRun() throws Exception {
        JetService service = getService();
        service.getLiveOperationRegistry().register(this);
    }

    @Override
    public final void run() throws Exception {
        try {
            doRun();
        } catch (Exception e) {
            logError(e);
            doSendResponse(e);
        }
    }

    protected abstract void doRun() throws Exception;

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        throw new UnsupportedOperationException();
    }

    public final void doSendResponse(Object value) {
        try {
            sendResponse(value);
        } finally {
            final JetService service = getService();
            service.getLiveOperationRegistry().deregister(this);
        }
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return isTopologicalFailure(throwable) ? THROW_EXCEPTION : super.onInvocationException(throwable);
    }

}
