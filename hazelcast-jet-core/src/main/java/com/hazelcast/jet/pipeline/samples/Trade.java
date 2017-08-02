/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.samples;

import java.io.Serializable;

/**
 * Javadoc pending.
 */
class Trade implements Serializable {

    private int productId;
    int brokerId;
    private int classId;

    Trade(int classId, int productId, int brokerId) {
        this.productId = productId;
        this.brokerId = brokerId;
        this.classId = classId;
    }

    int productId() {
        return productId;
    }

    int brokerId() {
        return brokerId;
    }

    int classId() {
        return classId;
    }

    @Override
    public boolean equals(Object obj) {
        Trade that;
        return obj instanceof Trade
                && this.productId == (that = (Trade) obj).productId
                && this.brokerId == (that = (Trade) obj).brokerId
                && this.classId == that.classId;
    }

    @Override
    public int hashCode() {
        int hc = 17;
        hc = 73 * hc + productId;
        hc = 73 * hc + brokerId;
        hc = 73 * hc + classId;
        return hc;
    }

    @Override
    public String toString() {
        return "Trade{productId=" + productId + ", brokerId=" + brokerId + ", classId=" + classId + '}';
    }
}
