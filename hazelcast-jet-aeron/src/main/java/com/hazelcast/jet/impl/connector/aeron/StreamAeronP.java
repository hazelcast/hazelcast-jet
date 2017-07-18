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
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.nio.Address;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class StreamAeronP<T> extends AbstractProcessor {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(100));


    private final Aeron aeron;

    private final String channel;

    private final List<Integer> streamIds;

    private final Function<byte[], T> mapperF;

    private final int fragmentLimit;

    private CompletableFuture<Void> jobFuture;

    private List<Subscription> subscriptionList;

    private FragmentHandler fragmentHandler;

    private long idle;

    StreamAeronP(Aeron aeron, String channel, List<Integer> streamIds,
                 Function<byte[], T> mapperF, int fragmentLimit) {
        this.aeron = aeron;
        this.channel = channel;
        this.streamIds = streamIds;
        this.mapperF = mapperF;
        this.fragmentLimit = fragmentLimit;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        jobFuture = context.jobFuture();
        subscriptionList = streamIds
                .stream()
                .map(i -> aeron.addSubscription(channel, i))
                .collect(toList());

        fragmentHandler = new FragmentAssembler(
                (buffer, offset, length, header) -> {
                    byte[] data = new byte[length];
                    buffer.getBytes(offset, data);
                    emit(mapperF.apply(data));
                    idle = -1;
                });
    }

    @Override
    public boolean complete() {
        subscriptionList.forEach(s -> s.poll(fragmentHandler, fragmentLimit));
        IDLER.idle(idle++);
        return jobFuture.isDone();
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private static ProcessorSupplier noopSupplier() {
        return count -> range(0, count).mapToObj(i -> Processors.noop().get()).collect(toList());
    }

    public static class MetaSupplier<T> implements ProcessorMetaSupplier {

        private static final long serialVersionUID = 1L;

        private final String directoryName;

        private final String channel;

        private final Set<Integer> streamIds;

        private final DistributedFunction<byte[], T> mapperF;

        private final int fragmentLimit;

        public MetaSupplier(String directoryName, String channel, Set<Integer> streamIds,
                            DistributedFunction<byte[], T> mapperF, int fragmentLimit) {
            this.directoryName = directoryName;
            this.channel = channel;
            this.streamIds = streamIds;
            this.mapperF = mapperF;
            this.fragmentLimit = fragmentLimit;
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            int[] count = new int[1];
            Map<Address, List<Integer>> streamIdMap = streamIds
                    .stream()
                    .collect(groupingBy(i -> addresses.get(count[0]++ % addresses.size())));
            return address -> {
                List<Integer> streamIds = streamIdMap.get(address);
                return streamIds == null
                        ? noopSupplier()
                        : new Supplier<>(directoryName, channel, streamIds, mapperF, fragmentLimit);
            };
        }
    }

    static class Supplier<T> implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final String directoryName;

        private final String channel;

        private final List<Integer> streamIds;

        private final DistributedFunction<byte[], T> mapperF;

        private final int fragmentLimit;

        private transient Aeron aeron;

        Supplier(String directoryName, String channel, List<Integer> streamIds,
                 DistributedFunction<byte[], T> mapperF, int fragmentLimit) {
            this.directoryName = directoryName;
            this.channel = channel;
            this.streamIds = streamIds;
            this.mapperF = mapperF;
            this.fragmentLimit = fragmentLimit;
        }

        @Override
        public void init(@Nonnull Context context) {
            Aeron.Context ctx = new Aeron.Context();
            ctx.aeronDirectoryName(directoryName);
            aeron = Aeron.connect(ctx);
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int processorCount) {
            int[] count = new int[1];
            Map<Integer, List<Integer>> streamIdMap = streamIds
                    .stream()
                    .collect(groupingBy(i -> count[0]++ % processorCount));

            range(0, processorCount).forEach(i -> streamIdMap.putIfAbsent(i, emptyList()));

            return streamIdMap
                    .values().stream()
                    .map(sIds -> sIds.isEmpty()
                            ? Processors.noop().get()
                            : new StreamAeronP<>(aeron, channel, sIds, mapperF, fragmentLimit))
                    .collect(toList());
        }
    }

}
