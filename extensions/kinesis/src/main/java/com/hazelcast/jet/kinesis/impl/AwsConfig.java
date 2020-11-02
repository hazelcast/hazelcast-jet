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
package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;

import javax.annotation.Nullable;
import java.io.Serializable;

public class AwsConfig implements Serializable { //todo: is it worth to use better serialization?

    private static final int CONNECTION_TIMEOUT = 1000;

    @Nullable
    private final String endpoint;

    @Nullable
    private final String region;

    @Nullable
    private final String accessKey;

    @Nullable
    private final String secretKey;

    public AwsConfig(
            @Nullable String endpoint,
            @Nullable String region,
            @Nullable String accessKey,
            @Nullable String secretKey
    ) {
        this.endpoint = endpoint;
        this.region = region;

        if (accessKey == null ^ secretKey == null) {
            throw new IllegalArgumentException("AWS access and secret keys must be specified together");
        }
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    @Nullable
    public String getEndpoint() {
        return endpoint;
    }

    @Nullable
    public String getRegion() {
        return region;
    }

    @Nullable
    public String getAccessKey() {
        return accessKey;
    }

    @Nullable
    public String getSecretKey() {
        return secretKey;
    }

    public AmazonKinesisAsync buildClient() {
        AmazonKinesisAsyncClientBuilder builder = AmazonKinesisAsyncClientBuilder.standard();
        if (endpoint == null) {
            if (region != null) {
                builder.setRegion(region);
            }
        } else {
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
        }

        builder.withCredentials(accessKey == null ? new DefaultAWSCredentialsProviderChain() :
                new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))
        );

        builder.withClientConfiguration(
                new ClientConfiguration()
                        .withMaxErrorRetry(0)
                        .withConnectionTimeout(CONNECTION_TIMEOUT)); //todo: need to have proper retry policy

        return builder.build();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(AwsConfig.class.getName()).append("(");
        boolean first = true;
        if (endpoint != null) {
            sb.append("endpoint: ").append(endpoint);
            sb.append(", region: ").append(region);
            first = false;
        }
        if (accessKey != null) {
            if (!first) {
                sb.append(", ");
            }
            sb.append("access key: ").append(accessKey);
            sb.append(", secret key: ").append(secretKey);

        }
        sb.append(")");
        return sb.toString();
    }
}
