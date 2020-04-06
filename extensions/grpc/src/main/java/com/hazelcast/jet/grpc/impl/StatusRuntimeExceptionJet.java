package com.hazelcast.jet.grpc.impl;

import com.hazelcast.jet.JetException;
import io.grpc.StatusRuntimeException;

/**
 * {@link io.grpc.StatusRuntimeException} breaks the Serializable contract, see
 * <a href="https://github.com/grpc/grpc-java/issues/1913">gRPC Issue #1913</a>.
 * Jet replaces it with a serializable one.
 */
public class StatusRuntimeExceptionJet extends JetException {
    StatusRuntimeExceptionJet(StatusRuntimeException brokenGrpcException) {
        super(brokenGrpcException.getMessage(), brokenGrpcException.getCause());
        setStackTrace(brokenGrpcException.getStackTrace());
    }
}
