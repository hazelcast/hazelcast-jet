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

package com.hazelcast.jet.impl.dag.sink;


import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.impl.util.SettableFuture;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.ShufflingStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;

public abstract class AbstractHazelcastWriter implements DataWriter {
    protected final DefaultObjectIOStream<Object> chunkInputStream;

    protected final SettableFuture<Boolean> future = SettableFuture.create();

    protected final InternalOperationService internalOperationService;

    protected final ContainerDescriptor containerDescriptor;

    protected final DefaultObjectIOStream<Object> chunkBuffer;

    protected final ILogger logger;

    protected volatile boolean isFlushed = true;

    private final int partitionId;
    private final NodeEngine nodeEngine;
    private final int awaitInSecondsTime;

    private final ShufflingStrategy shufflingStrategy;
    private final PartitionSpecificRunnable partitionSpecificRunnable = new PartitionSpecificRunnable() {
        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                processChunk(chunkInputStream);
                future.set(true);
            } catch (Throwable e) {
                future.setException(e);
            }
        }
    };
    private final PartitionSpecificRunnable partitionSpecificOpenRunnable = new PartitionSpecificRunnable() {
        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                onOpen();
                future.set(true);
            } catch (Throwable e) {
                future.setException(e);
            }
        }
    };
    private int lastConsumedCount;
    private boolean isClosed;

    protected AbstractHazelcastWriter(ContainerDescriptor containerDescriptor,
                                      int partitionId) {
        checkNotNull(containerDescriptor);
        this.partitionId = partitionId;
        this.nodeEngine = containerDescriptor.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        this.containerDescriptor = containerDescriptor;
        JobConfig jobConfig = containerDescriptor.getConfig();
        this.awaitInSecondsTime = jobConfig.getSecondsToAwait();
        this.internalOperationService = (InternalOperationService) this.nodeEngine.getOperationService();
        int tupleChunkSize = jobConfig.getChunkSize();
        this.chunkBuffer = new DefaultObjectIOStream<Object>(new Object[tupleChunkSize]);
        this.chunkInputStream = new DefaultObjectIOStream<Object>(new Object[tupleChunkSize]);
        this.shufflingStrategy = null;
    }

    @Override
    public boolean consume(ProducerInputStream<Object> chunk) throws Exception {
        return consumeChunk(chunk) > 0;
    }

    private void pushWriteRequest() {
        this.future.reset();
        this.internalOperationService.execute(this.partitionSpecificRunnable);
        this.isFlushed = false;
    }

    @Override
    public int consumeChunk(ProducerInputStream<Object> chunk) throws Exception {
        this.chunkInputStream.consumeStream(chunk);
        pushWriteRequest();
        this.lastConsumedCount = chunk.size();
        return chunk.size();
    }

    @Override
    public int consumeObject(Object object) throws Exception {
        this.chunkBuffer.consume(object);
        this.lastConsumedCount = 1;
        return 1;
    }

    @Override
    public int flush() {
        try {
            return chunkBuffer.size() > 0 ? consumeChunk(chunkBuffer) : 0;
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }
    }

    protected abstract void processChunk(ProducerInputStream<Object> inputStream);

    @Override
    public int getPartitionId() {
        return this.partitionId;
    }

    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            try {
                flush();
            } finally {
                this.isClosed = true;
                onClose();
            }
        }
    }

    protected void onOpen() {

    }

    protected void onClose() {

    }

    @Override
    public void open() {
        this.future.reset();
        this.isFlushed = true;
        this.isClosed = false;

        this.internalOperationService.execute(this.partitionSpecificOpenRunnable);

        try {
            this.future.get(this.awaitInSecondsTime, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public boolean isFlushed() {
        if (this.isFlushed) {
            return true;
        } else {
            try {
                if (this.future.isDone()) {
                    try {
                        future.get();
                        return true;
                    } finally {
                        this.chunkBuffer.reset();
                        this.isFlushed = true;
                        this.chunkInputStream.reset();
                    }
                }

                return false;
            } catch (Exception e) {
                throw JetUtil.reThrow(e);
            }
        }
    }

    @Override
    public int lastConsumedCount() {
        return this.lastConsumedCount;
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return this.shufflingStrategy;
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return DefaultHashingStrategy.INSTANCE;
    }


}
