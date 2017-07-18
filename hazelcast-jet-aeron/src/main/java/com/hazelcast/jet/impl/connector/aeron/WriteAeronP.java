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

package com.hazelcast.jet.impl.connector.aeron;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class WriteAeronP<T> extends AbstractProcessor {

    private static final int BUFFER_CAPACITY = 256;
    private static final int BUFFER_ALIGNMENT = 64;

    private final Publication publication;
    private final UnsafeBuffer buffer;
    private final Function<T, byte[]> mapperF;

    WriteAeronP(Publication publication, Function<T, byte[]> mapperF) {
        this.publication = publication;
        this.mapperF = mapperF;
        this.buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(BUFFER_CAPACITY, BUFFER_ALIGNMENT));
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        byte[] data = mapperF.apply((T) item);
        buffer.putBytes(0, data);
        long result = publication.offer(buffer, 0, data.length);
        if (result >= 0) {
            return true;
        }
        if (result == Publication.CLOSED) {
            throw new IllegalStateException("Publication is closed");
        }
        return false;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    public static class Supplier<T> implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final String directoryName;

        private final String channel;

        private final int streamId;

        private final DistributedFunction<T, byte[]> mapperF;

        private transient Publication publication;

        public Supplier(String directoryName, String channel, int streamId,
                        DistributedFunction<T, byte[]> mapperF) {
            this.directoryName = directoryName;
            this.channel = channel;
            this.streamId = streamId;
            this.mapperF = mapperF;
        }


        @Override
        public void init(@Nonnull Context context) {
            Aeron.Context ctx = new Aeron.Context();
            ctx.aeronDirectoryName(directoryName);
            Aeron aeron = Aeron.connect(ctx);
            publication = aeron.addPublication(channel, streamId);
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return range(0, count)
                    .mapToObj(i -> new WriteAeronP<>(publication, mapperF))
                    .collect(toList());
        }
    }


}
