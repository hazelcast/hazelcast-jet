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

package com.hazelcast.jet.impl.container.task.processors.shuffling;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.impl.actor.ObjectConsumer;
import com.hazelcast.jet.impl.container.ContainerContext;
import com.hazelcast.jet.processor.ContainerProcessor;

public class ShuffledReceiverConsumerTaskProcessor extends ShuffledConsumerTaskProcessor {
    public ShuffledReceiverConsumerTaskProcessor(ObjectConsumer[] consumers,
                                                 ContainerProcessor processor,
                                                 ContainerContext containerContext,
                                                 ProcessorContext processorContext,
                                                 int taskID) {
        super(consumers, processor, containerContext, processorContext, taskID, true);
    }
}
