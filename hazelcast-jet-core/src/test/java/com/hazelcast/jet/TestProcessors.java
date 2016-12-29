/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.impl.connector.AbstractProducer;

public class TestProcessors {

    public static class Identity extends AbstractProcessor {
        @Override
        protected boolean process(int ordinal, Object item) {
            emit(item);
            return true;
        }
    }

    public static class BlockingIdentity extends Identity {
        @Override
        public boolean isCooperative() {
            return false;
        }
    }


    public static class FaultyProducer extends AbstractProducer {

        private final RuntimeException e;

        public FaultyProducer(RuntimeException e) {
            this.e = e;
        }

        @Override
        public boolean complete() {
            throw e;
        }
    }
}
