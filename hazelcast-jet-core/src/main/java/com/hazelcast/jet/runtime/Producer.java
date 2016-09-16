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

package com.hazelcast.jet.runtime;

/**
 * This is an abstract interface for each producer in the system
 * which produce data objects
 */
public interface Producer {
    /**
     * @return last produced object's count
     */
    int lastProducedCount();

    /**
     * @return producer's name
     */
    String getName();

    /**
     * Opens this producer
     */
    void open();

    /**
     * Closes this producer
     */
    void close();

    /**
     * Method to produce an abstract entry
     *
     * @return produced entry
     * @throws Exception if any exception
     */
    Object[] produce() throws Exception;

    /**
     * Register a handler which will be run when the producer is completed
     */
    void registerCompletionHandler(ProducerCompletionHandler runnable);
}
