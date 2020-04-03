/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.grpc.impl;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

public final class GrpcUtil {

    private GrpcUtil() {

    }

    public static Throwable wrapGrpcException(Throwable exception) {
        // some gRPC exceptions break Serializable contract, handle these explicitly
        // see: https://github.com/grpc/grpc-java/issues/1913
        if (exception instanceof StatusException || exception instanceof StatusRuntimeException) {
            // not serializable exceptions
            exception = new JetException("Call to gRPC service failed with "
                    + ExceptionUtil.stackTraceToString(exception));
        }
        return exception;
    }
}
