package com.hazelcast.jet.grpc.impl;

import com.hazelcast.jet.JetException;
import io.grpc.StatusException;

/**
 * {@link io.grpc.StatusException} breaks the Serializable contract, see
 * <a href="https://github.com/grpc/grpc-java/issues/1913">gRPC Issue #1913</a>.
 * Jet replaces it with a serializable one.
 */
public class StatusExceptionJet extends JetException {
    StatusExceptionJet(StatusException brokenGrpcException) {
        super(brokenGrpcException.getMessage(), brokenGrpcException.getCause());
        setStackTrace(brokenGrpcException.getStackTrace());
    }
}
