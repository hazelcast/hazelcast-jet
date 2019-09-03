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

package com.hazelcast.jet.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.testcontainers.containers.GenericContainer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Set;

/**
 * todo add proper javadoc
 */
public class S3MockContainer extends GenericContainer<S3MockContainer> {

    public static final String VERSION = "2.1.15";
    public static final Integer S3_PORT = 9191;

    private static final String IMAGE_NAME = "adobe/s3mock";

    public S3MockContainer() {
        this(VERSION);
    }

    public S3MockContainer(String version) {
        super(IMAGE_NAME + ":" + version);
    }

    @Override
    protected void configure() {
        addExposedPort(S3_PORT);
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return Collections.singleton(getMappedPort(S3_PORT));
    }

    String endpointURL() {
        return "https://" + getContainerIpAddress() + ":" + getMappedPort(S3_PORT);
    }

    AmazonS3 client() {
        return client(endpointURL());
    }

    static AmazonS3 client(String endpointURL) {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar")))
                .withClientConfiguration(ignoringInvalidSslCertificates(new ClientConfiguration()))
                .withEndpointConfiguration(new EndpointConfiguration(endpointURL, "us-east-1"))
                .enablePathStyleAccess()
                .build();
    }

    private static ClientConfiguration ignoringInvalidSslCertificates(ClientConfiguration clientConfiguration) {
        clientConfiguration
                .getApacheHttpClientConfig()
                .withSslSocketFactory(new SSLConnectionSocketFactory(
                        createBlindlyTrustingSslContext(),
                        NoopHostnameVerifier.INSTANCE));

        return clientConfiguration;
    }

    private static SSLContext createBlindlyTrustingSslContext() {
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(
                    null,
                    new TrustManager[]{
                            new X509ExtendedTrustManager() {
                                @Override
                                public X509Certificate[] getAcceptedIssuers() {
                                    return null;
                                }

                                @Override
                                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                                }

                                @Override
                                public void checkClientTrusted(X509Certificate[] arg0, String arg1, SSLEngine arg2) {
                                }

                                @Override
                                public void checkClientTrusted(X509Certificate[] arg0, String arg1, Socket arg2) {
                                }

                                @Override
                                public void checkServerTrusted(X509Certificate[] arg0, String arg1, SSLEngine arg2) {
                                }

                                @Override
                                public void checkServerTrusted(X509Certificate[] arg0, String arg1, Socket arg2) {
                                }

                                @Override
                                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                                }

                            }
                    },
                    new java.security.SecureRandom()
            );
            return sc;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException("Unexpected exception", e);
        }
    }
}
