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

import com.amazonaws.regions.Regions;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Holds credentials and region information for Amazon AWS S3 client
 */
public class S3Parameters implements Serializable {

    private String accessKeyId;
    private String accessKeySecret;
    private Regions region;

    private S3Parameters(
            String accessKeyId,
            String accessKeySecret,
            Regions region
    ) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.region = region;
    }

    public static S3Parameters create(
            @Nonnull String accessKeyId,
            @Nonnull String accessKeySecret
    ) {
        return new S3Parameters(accessKeyId, accessKeySecret, Regions.DEFAULT_REGION);
    }

    public static S3Parameters create(
            @Nonnull String accessKeyId,
            @Nonnull String accessKeySecret,
            @Nonnull Regions region
    ) {
        return new S3Parameters(accessKeyId, accessKeySecret, region);
    }

    String getAccessKeyId() {
        return accessKeyId;
    }

    String getAccessKeySecret() {
        return accessKeySecret;
    }

    Regions getRegion() {
        return region;
    }
}
