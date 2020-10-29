package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;

import javax.annotation.Nullable;
import java.io.Serializable;

public class AwsConfig implements Serializable { //todo: is it worth to use better serialization?

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
        if (endpoint == null ^ region == null) {
            throw new IllegalArgumentException("AWS endpoint and region overrides must be specified together");
        }
        this.endpoint = endpoint;
        this.region = region;

        if (accessKey == null ^ secretKey == null) {
            throw new IllegalArgumentException("AWS access and secret keys must be specified together");
        }
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public AmazonKinesisAsync buildClient() {
        AmazonKinesisAsyncClientBuilder builder = AmazonKinesisAsyncClientBuilder.standard();
        if (endpoint != null) {
            builder.withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, region)
            );
        }
        if (accessKey != null) {
            builder.withCredentials(
                    new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))
            );
        }
        builder.withClientConfiguration(
                new ClientConfiguration()
                        .withMaxErrorRetry(0)
                        .withConnectionTimeout(1000)); //todo: need to have proper retry policy

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
