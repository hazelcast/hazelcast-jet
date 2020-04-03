package com.hazelcast.jet.grpc.impl;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

public final class Util {

    public static Exception wrapGrpcException(Exception exception) {
        // some gRPC exceptions break Serializable contract, handle these explicitly
        // see: https://github.com/grpc/grpc-java/issues/1913
        if (exception instanceof StatusException || exception instanceof StatusRuntimeException) {
            // not serializable exceptions
            exception = new JetException(ExceptionUtil.stackTraceToString(exception));
        }
        return exception;
    }
}
