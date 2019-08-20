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

package com.hazelcast.jet.impl.util;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConfigXmlGenerator;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapPutAllCodec;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Predicate;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.Util.toCompletableFuture;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.lang.Math.log10;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public final class Util {

    private static final int BUFFER_SIZE = 1 << 15;
    private static final DateTimeFormatter LOCAL_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final Pattern TRAILING_NUMBER_PATTERN = Pattern.compile("(.*)-([0-9]+)");
    public static final float PUT_ALL_INITIAL_SIZE_MAGIC = 20f;

    private Util() {
    }

    public static <T> Supplier<T> memoize(Supplier<T> onceSupplier) {
        return new MemoizingSupplier<>(onceSupplier);
    }

    public static <T> Supplier<T> memoizeConcurrent(Supplier<T> onceSupplier) {
        return new ConcurrentMemoizingSupplier<>(onceSupplier);
    }

    public static <T> T uncheckCall(@Nonnull Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    public static void uncheckRun(@Nonnull RunnableExc r) {
        try {
            r.run();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    /**
     * This method will generate an {@link ExecutionCallback} which
     * allows to asynchronously get notified when the execution is completed,
     * either successfully or with error by calling {@code onResponse} on success
     * and {@code onError} on error respectively.
     *
     * @param onResponse function to call when execution is completed successfully
     * @param onError function to call when execution is completed with error
     * @param <T> type of the response
     * @return {@link ExecutionCallback}
     */
    public static <T> ExecutionCallback<T> callbackOf(@Nonnull Consumer<T> onResponse,
                                                      @Nonnull Consumer<Throwable> onError) {
        return new ExecutionCallback<T>() {
            @Override
            public void onResponse(T o) {
                onResponse.accept(o);
            }

            @Override
            public void onFailure(Throwable throwable) {
                onError.accept(throwable);
            }
        };
    }

    /**
     * This method will generate an {@link ExecutionCallback} which allows to
     * asynchronously get notified when the execution is completed, either
     * successfully or with an error.
     *
     * @param callback BiConsumer to call when execution is completed. Only one
     *                of the passed values will be non-null, except for the
     *                case the normal result is null, in which case both values
     *                will be null
     * @param <T> type of the response
     * @return {@link ExecutionCallback}
     */
    public static <T> ExecutionCallback<T> callbackOf(BiConsumer<T, Throwable> callback) {
        return new ExecutionCallback<T>() {
            @Override
            public void onResponse(T o) {
                callback.accept(o, null);
            }

            @Override
            public void onFailure(Throwable throwable) {
                callback.accept(null, throwable);
            }
        };
    }

    /**
     * Atomically increment the {@code value} by {@code increment}, unless
     * the value after increment would exceed the {@code limit}.
     *
     * @param limit maximum value the {@code value} can take (inclusive)
     * @return {@code true}, if successful, {@code false}, if {@code limit} would be exceeded.
     */
    @CheckReturnValue
    public static boolean tryIncrement(AtomicInteger value, int increment, int limit) {
        int prev;
        int next;
        do {
            prev = value.get();
            next = prev + increment;
            if (next > limit) {
                return false;
            }
        } while (!value.compareAndSet(prev, next));
        return true;
    }

    public static boolean existsDistributedObject(NodeEngine nodeEngine, String serviceName, String objectName) {
        return nodeEngine.getProxyService()
                  .getDistributedObjectNames(serviceName)
                  .contains(objectName);
    }

    public interface RunnableExc<T extends Exception> {
        void run() throws T;
    }

    @Nonnull
    public static List<Address> getRemoteMembers(@Nonnull NodeEngine engine) {
        final Member localMember = engine.getLocalMember();
        return engine.getClusterService().getMembers().stream()
                     .filter(m -> !m.equals(localMember))
                     .map(Member::getAddress)
                     .collect(toList());
    }

    public static Connection getMemberConnection(@Nonnull NodeEngine engine, @Nonnull Address memberAddr) {
        return ((NodeEngineImpl) engine).getNode().getEndpointManager().getConnection(memberAddr);
    }

    public static JetInstance getJetInstance(NodeEngine nodeEngine) {
        return nodeEngine.<JetService>getService(JetService.SERVICE_NAME).getJetInstance();
    }

    @Nonnull
    public static BufferObjectDataOutput createObjectDataOutput(@Nonnull NodeEngine engine) {
        return ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataOutput(BUFFER_SIZE);
    }

    @Nonnull
    public static BufferObjectDataInput createObjectDataInput(@Nonnull NodeEngine engine, @Nonnull byte[] buf) {
        return ((InternalSerializationService) engine.getSerializationService())
                .createObjectDataInput(buf);
    }

    @Nonnull
    public static byte[] readFully(@Nonnull InputStream in) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] b = new byte[BUFFER_SIZE];
            for (int len; (len = in.read(b)) != -1; ) {
                out.write(b, 0, len);
            }
            return out.toByteArray();
        }
    }

    public static void writeList(@Nonnull ObjectDataOutput output, @Nonnull List list) throws IOException {
        output.writeInt(list.size());
        for (Object o : list) {
            output.writeObject(o);
        }
    }

    @Nonnull
    public static <E> List<E> readList(@Nonnull ObjectDataInput output) throws IOException {
        int length = output.readInt();
        List<E> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(output.readObject());
        }
        return list;
    }

    public static long addClamped(long a, long b) {
        long sum = a + b;
        return sumHadOverflow(a, b, sum)
                ? (a >= 0 ? Long.MAX_VALUE : Long.MIN_VALUE)
                : sum;
    }

    /**
     * Calculates {@code a - b}, returns {@code Long.MAX_VALUE} if the result
     * would overflow
     *
     * @param a the amount
     * @param b the value to subtract
     * @return {@code a - b}, clamped
     */
    public static long subtractClamped(long a, long b) {
        long diff = a - b;
        return diffHadOverflow(a, b, diff)
                ? (a >= 0 ? Long.MAX_VALUE : Long.MIN_VALUE)
                : diff;
    }

    // Hacker's Delight, 2nd Ed, 2-13: overflow has occurred iff
    // operands have the same sign which is opposite of the result
    public static boolean sumHadOverflow(long a, long b, long sum) {
        return ((a ^ sum) & (b ^ sum)) < 0;
    }

    // Hacker's Delight, 2nd Ed, 2-13: overflow has occurred iff operands have
    // opposite signs and result has opposite sign of left-hand operand
    public static boolean diffHadOverflow(long a, long b, long diff) {
        return ((a ^ b) & (a ^ diff)) < 0;
    }

    /**
     * Checks that the {@code object} implements {@link Serializable} and is
     * correctly serializable by actually trying to serialize it. This will
     * reveal some non-serializable field early.
     *
     * @param object     object to check
     * @param objectName object description for the exception
     * @throws IllegalArgumentException if {@code object} is not serializable
     */
    public static void checkSerializable(Object object, String objectName) {
        if (object == null) {
            return;
        }
        if (!(object instanceof Serializable)) {
            throw new IllegalArgumentException('"' + objectName + "\" must implement Serializable");
        }
        try (ObjectOutputStream os = new ObjectOutputStream(new NullOutputStream())) {
            os.writeObject(object);
        } catch (NotSerializableException | InvalidClassException e) {
            throw new IllegalArgumentException("\"" + objectName + "\" must be serializable", e);
        } catch (IOException e) {
            // never really thrown, as the underlying stream never throws it
            throw new JetException(e);
        }
    }

    /**
     * Distributes the owned partitions to processors in a round-robin fashion.
     * If owned partition size is smaller than processor count, an empty list
     * is put for the rest of the processors.
     *
     * @param count count of processors
     * @param ownedPartitions list of owned partitions
     * @return a map of which has partition index as key and list of partition ids as value
     */
    public static Map<Integer, List<Integer>> processorToPartitions(int count, List<Integer> ownedPartitions) {
        Map<Integer, List<Integer>> processorToPartitions = range(0, ownedPartitions.size())
                .mapToObj(i -> entry(i, ownedPartitions.get(i)))
                .collect(groupingBy(e -> e.getKey() % count, mapping(Map.Entry::getValue, toList())));

        for (int processor = 0; processor < count; processor++) {
            processorToPartitions.putIfAbsent(processor, emptyList());
        }
        return processorToPartitions;
    }

    /**
     * From an imaginary set of integers {@code (0, 1, ..., objectCount-1)}
     * returns {@code index}-th disjoint subset out of {@code count} subsets.
     * The assignment of objects to subset is done in round-robin fashion.
     * <p>
     * For example, if {@code objectCount==3} and {@code count==2}, then for
     * {@code index==0} it will return {@code {0, 2}} and for {@code index==1}
     * it will return {@code {1}}.
     * <p>
     * It's used to assign partitions to processors.
     *
     * @param objectCount total number of objects to distribute
     * @param count total number of subsets
     * @param index index of the requested subset
     * @return an array with assigned objects
     */
    public static int[] roundRobinPart(int objectCount, int count, int index) {
        if (objectCount < 0 || index < 0 || count < 1 || index >= count) {
            throw new IllegalArgumentException("objectCount=" + objectCount + ", count=" + count + ", index=" + index);
        }

        int[] res = new int[objectCount / count + (objectCount % count > index ? 1 : 0)];
        for (int i = 0, j = index; j < objectCount; i++, j += count) {
            res[i] = j;
        }
        return res;
    }

    private static class NullOutputStream extends OutputStream {
        @Override
        public void write(int b) {
            // do nothing
        }
    }

    /*
 * The random number generator used by this class to create random
 * based UUIDs. In a holder class to defer initialization until needed.
 */
    private static class Holder {
        static final SecureRandom NUMBER_GENERATOR = new SecureRandom();
    }

    public static long secureRandomNextLong() {
        return Holder.NUMBER_GENERATOR.nextLong();
    }

    public static String jobNameAndExecutionId(String jobName, long executionId) {
        return "job '" + jobName + "', execution " + idToString(executionId);
    }

    public static String jobIdAndExecutionId(long jobId, long executionId) {
        return "job " + idToString(jobId) + ", execution " + idToString(executionId);
    }

    public static ZonedDateTime toZonedDateTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
    }

    public static LocalDateTime toLocalDateTime(long timestamp) {
        return toZonedDateTime(timestamp).toLocalDateTime();
    }

    public static String toLocalTime(long timestamp) {
        return toZonedDateTime(timestamp).toLocalTime().format(LOCAL_TIME_FORMATTER);
    }

    public static <K, V> EntryProcessor<K, V> entryProcessor(
            BiFunctionEx<? super K, ? super V, ? extends V> remappingFunction
    ) {
        return new AbstractEntryProcessor<K, V>() {
            @Override
            public Object process(Entry<K, V> entry) {
                V newValue = remappingFunction.apply(entry.getKey(), entry.getValue());
                entry.setValue(newValue);
                return newValue;
            }
        };
    }

    public static boolean isMemberInstance(HazelcastInstance instance) {
        return instance.getLocalEndpoint() instanceof Member;
    }

    /**
     * Sequentially search through an array, return the index of first {@code
     * needle} element in {@code haystack} or -1, if not found.
     */
    public static int arrayIndexOf(int needle, int[] haystack) {
        for (int i = 0; i < haystack.length; i++) {
            if (haystack[i] == needle) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns a future which is already completed with the supplied exception.
     */
    // replace with CompletableFuture.failedFuture(e) once we depend on java9+
    public static <T> CompletableFuture<T> exceptionallyCompletedFuture(@Nonnull Throwable exception) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(exception);
        return future;
    }

    /**
     * Logs a late event that was dropped.
     */
    public static void logLateEvent(ILogger logger, long currentWm, @Nonnull Object item) {
        if (!logger.isInfoEnabled()) {
            return;
        }
        if (item instanceof JetEvent) {
            JetEvent event = (JetEvent) item;
            logger.info(
                    String.format("Event dropped, late by %d ms. currentWatermark=%s, eventTime=%s, event=%s",
                            currentWm - event.timestamp(), toLocalTime(currentWm), toLocalTime(event.timestamp()),
                            event.payload()
                    ));
        } else {
            logger.info(String.format(
                    "Late event dropped. currentWatermark=%s, event=%s", new Watermark(currentWm), item
            ));
        }
    }

    /**
     * Calculate greatest common divisor of a series of integer numbers. Returns
     * 0, if the number of values is 0.
     */
    public static long gcd(long... values) {
        long res = 0;
        for (long value : values) {
            res = gcd(res, value);
        }
        return res;
    }

    /**
     * Calculate greatest common divisor of two integer numbers.
     */
    public static long gcd(long a, long b) {
        a = abs(a);
        b = abs(b);
        if (b == 0) {
            return a;
        }
        return gcd(b, a % b);
    }

    public static void lazyIncrement(AtomicLong counter) {
        lazyAdd(counter, 1);
    }

    public static void lazyIncrement(AtomicLongArray counters, int index) {
        lazyAdd(counters, index, 1);
    }

    /**
     * Adds {@code addend} to the counter, using {@code lazySet}. Useful for
     * incrementing {@linkplain com.hazelcast.internal.metrics.Probe probes}
     * if only one thread is updating the value.
     */
    public static void lazyAdd(AtomicLong counter, long addend) {
        counter.lazySet(counter.get() + addend);
    }

    /**
     * Adds {@code addend} to the counter, using {@code lazySet}. Useful for
     * incrementing {@linkplain com.hazelcast.internal.metrics.Probe probes}
     * if only one thread is updating the value.
     */
    public static void lazyAdd(AtomicLongArray counters, int index, long addend) {
        counters.lazySet(index, counters.get(index) + addend);
    }

    /**
     * Adds items of the collection. Faster than {@code
     * collection.stream().mapToInt(toIntF).sum()} and equal to plain old loop
     * in performance. Crates no GC litter (if you use non-capturing lambda for
     * {@code toIntF}, else new lambda instance is created for each call).
     */
    public static <E> int sum(Collection<E> collection, ToIntFunction<E> toIntF) {
        int sum = 0;
        for (E e : collection) {
            sum += toIntF.applyAsInt(e);
        }
        return sum;
    }

    /**
     * Escapes the vertex name for graphviz by prefixing the quote character
     * with backslash.
     */
    public static String escapeGraphviz(String value) {
        return value.replace("\"", "\\\"");
    }

    /**
     * Converts {@link ClientConfig} to xml representation using {@link
     * ClientConfigXmlGenerator}.
     */
    public static String asXmlString(ClientConfig clientConfig) {
        return clientConfig == null ? null : ClientConfigXmlGenerator.generate(clientConfig);
    }

    /**
     * Converts client-config xml string to {@link ClientConfig} using {@link
     * XmlClientConfigBuilder}.
     */
    public static ClientConfig asClientConfig(String xml) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        return new XmlClientConfigBuilder(inputStream).build();
    }

    public static CompletableFuture<Void> copyMapUsingJob(JetInstance instance, int queueSize,
                                                          String sourceMap, String targetMap) {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("readMap(" + sourceMap + ')', readMapP(sourceMap));
        Vertex sink = dag.newVertex("writeMap(" + targetMap + ')', writeMapP(targetMap));
        dag.edge(between(source, sink).setConfig(new EdgeConfig().setQueueSize(queueSize)));
        JobConfig jobConfig = new JobConfig()
                .setName("copy-" + sourceMap + "-to-" + targetMap);
        return instance.newJob(dag, jobConfig).getFuture();
    }

    /**
     * If the name ends with "-N", returns a new name where "-N" is replaced
     * with "-M" where M = N + 1. If N is too large for {@code int}, negative,
     * unparseable, not present or smaller than 2, then it's ignored and just
     * "-2" is appended.
     */
    public static String addOrIncrementIndexInName(String name) {
        Matcher m = TRAILING_NUMBER_PATTERN.matcher(name);
        int index = 2;
        if (m.matches()) {
            try {
                int newIndex = Integer.parseInt(m.group(2)) + 1;
                if (newIndex > 2) {
                    index = newIndex;
                    name = m.group(1);
                }
            } catch (NumberFormatException ignored) {
            }
        }
        return name + '-' + index;
    }

    public static <T> PredicateEx<T> wrapImdgPredicate(com.hazelcast.util.function.Predicate<T> predicate) {
        return new ImdgPredicateWrapper(predicate);
    }

    public static <T> com.hazelcast.util.function.Predicate<T> maybeUnwrapImdgPredicate(PredicateEx<T> predicate) {
        if (predicate instanceof ImdgPredicateWrapper) {
            return ((ImdgPredicateWrapper<T>) predicate).wrapped;
        }
        return predicate;
    }

    public static FunctionEx wrapImdgFunction(com.hazelcast.util.function.Function function) {
        return new ImdgFunctionWrapper(function);
    }

    public static <T, R> com.hazelcast.util.function.Function<T, R> maybeUnwrapImdgFunction(FunctionEx<T, R> function) {
        if (function instanceof ImdgFunctionWrapper) {
            return ((ImdgFunctionWrapper<T, R>) function).wrapped;
        }
        return function;
    }

    private static final class ImdgPredicateWrapper<T> implements PredicateEx<T> {
        private final com.hazelcast.util.function.Predicate<T> wrapped;

        ImdgPredicateWrapper(Predicate<T> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public boolean testEx(T t) {
            return wrapped.test(t);
        }
    }

    private static final class ImdgFunctionWrapper<T, R> implements FunctionEx<T, R> {
        private final com.hazelcast.util.function.Function<T, R> wrapped;

        ImdgFunctionWrapper(com.hazelcast.util.function.Function<T, R> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public R applyEx(T t) {
            return wrapped.apply(t);
        }
    }

    public static String sanitizeLoggerNamePart(String name) {
        return name.replace('.', '_');
    }

    public static <T extends Exception> void doWithClassLoader(ClassLoader cl, RunnableExc<T> action) throws T {
        Thread currentThread = Thread.currentThread();
        ClassLoader previousCl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(cl);
        try {
            action.run();
        } finally {
            currentThread.setContextClassLoader(previousCl);
        }
    }

    /**
     * Async version of {@code IMap.putAll()}. This is based on IMDG's code and
     * currently does not invalidate the near cache.
     *
     * TODO remove this method once https://github.com/hazelcast/hazelcast/pull/15463 is released

     * @param targetIMap imap to write to
     * @param items      items to add
     */
    public static <K, V> CompletionStage<Void> mapPutAllAsync(
            @Nonnull IMap<K, V> targetIMap, Map<? extends K, ? extends V> items
    ) {
        if (items.isEmpty()) {
            return completedFuture(null);
        }
        if (items.size() == 1) {
            Entry<? extends K, ? extends V> onlyEntry = items.entrySet().iterator().next();
            return toCompletableFuture(targetIMap.setAsync(onlyEntry.getKey(), onlyEntry.getValue()));
        }

        if (targetIMap instanceof MapProxyImpl) {
            return mapPutAllAsync((MapProxyImpl<K, V>) targetIMap, items);
        } else if (targetIMap instanceof ClientMapProxy) {
            return mapPutAllAsync((ClientMapProxy<K, V>) targetIMap, items);
        } else {
            throw new RuntimeException("Unexpected map class: " + targetIMap.getClass().getName());
        }
    }

    private static <K, V> CompletionStage<Void> mapPutAllAsync(
            @Nonnull MapProxyImpl<K, V> targetMap,
            @Nonnull Map<? extends K, ? extends V> items
    ) {
        NodeEngine nodeEngine = targetMap.getNodeEngine();
        SerializationService serializationService = nodeEngine.getSerializationService();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();

        MapEntries[] entries = new MapEntries[partitionService.getPartitionCount()];
        // this is an educated guess for the initial size of the entries per partition, depending on the map size
        int initialSize = (int) ceil(PUT_ALL_INITIAL_SIZE_MAGIC * items.size() /
                partitionService.getPartitionCount() / log10(items.size()));

        for (Entry<? extends K, ? extends V> entry : items.entrySet()) {
            checkNotNull(entry.getKey(), "Null key is not allowed");
            checkNotNull(entry.getValue(), "Null value is not allowed");

            Data keyData = serializationService.toData(entry.getKey(), targetMap.getPartitionStrategy());
            int partitionId = partitionService.getPartitionId(keyData);
            MapEntries partitionEntries = entries[partitionId];
            if (partitionEntries == null) {
                partitionEntries = new MapEntries(initialSize);
                entries[partitionId] = partitionEntries;
            }

            partitionEntries.add(keyData, serializationService.toData(entry.getValue()));
        }

        int[] subPartitions = new int[memberPartitionsMap.values().stream().mapToInt(List::size).max().orElse(0)];
        MapEntries[] subEntries = new MapEntries[subPartitions.length];
        AtomicInteger completionCounter = new AtomicInteger(memberPartitionsMap.size());
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        ExecutionCallback<Map<Integer, Object>> callback = new ExecutionCallback<Map<Integer, Object>>() {
            @Override
            public void onResponse(Map<Integer, Object> response) {
                if (completionCounter.decrementAndGet() == 0) {
                    resultFuture.complete(null);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                resultFuture.completeExceptionally(t);
            }
        };

        for (Entry<Address, List<Integer>> entry : memberPartitionsMap.entrySet()) {
            List<Integer> memberPartitions = entry.getValue();
            int count = 0;
            for (int partitionId : memberPartitions) {
                if (entries[partitionId] != null) {
                    subPartitions[count] = partitionId;
                    subEntries[count] = entries[partitionId];
                    count++;
                }
            }
            if (count == 0) {
                callback.onResponse(null);
                continue;
            }
            int[] subPartitionsTrimmed = Arrays.copyOf(subPartitions, count);
            MapEntries[] subEntriesTrimmed = Arrays.copyOf(subEntries, count);

            if (count > 0) {
                OperationFactory factory = targetMap.getOperationProvider().createPutAllOperationFactory(
                        targetMap.getName(), subPartitionsTrimmed, subEntriesTrimmed);
                targetMap.getOperationService().invokeOnPartitionsAsync(SERVICE_NAME, factory,
                                asIntegerList(subPartitionsTrimmed))
                         .andThen(callback);
            }
        }

        return resultFuture;
    }

    private static <K, V> CompletionStage<Void> mapPutAllAsync(
            ClientMapProxy<K, V> targetMap,
            Map<? extends K, ? extends V> items
    ) {
        if (items.isEmpty()) {
            return completedFuture(null);
        }
        checkNotNull(targetMap, "Null argument map is not allowed");
        ClientPartitionService partitionService = targetMap.getContext().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Map<Integer, List<Map.Entry<Data, Data>>> entryMap = new HashMap<>(partitionCount);
        InternalSerializationService serializationService = targetMap.getContext().getSerializationService();

        for (Entry<? extends K, ? extends V> entry : items.entrySet()) {
            checkNotNull(entry.getKey(), "Null key is not allowed");
            checkNotNull(entry.getValue(), "Null value is not allowed");

            Data keyData = serializationService.toData(entry.getKey());
            int partitionId = partitionService.getPartitionId(keyData);
            entryMap
                    .computeIfAbsent(partitionId, k -> new ArrayList<>())
                    .add(new AbstractMap.SimpleEntry<>(keyData, serializationService.toData(entry.getValue())));
        }

        HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) targetMap.getContext().getHazelcastInstance();
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        AtomicInteger completionCounter = new AtomicInteger(entryMap.size());
        ExecutionCallback<ClientMessage> callback = new ExecutionCallback<ClientMessage>() {
            @Override
            public void onResponse(ClientMessage response) {
                if (completionCounter.decrementAndGet() == 0) {
                    resultFuture.complete(null);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                resultFuture.completeExceptionally(t);
            }
        };
        for (Entry<Integer, List<Map.Entry<Data, Data>>> partitionEntries : entryMap.entrySet()) {
            Integer partitionId = partitionEntries.getKey();
            // use setAsync if there's only one entry
            if (partitionEntries.getValue().size() == 1) {
                Entry<Data, Data> onlyEntry = partitionEntries.getValue().get(0);
                // cast to raw so that we can pass serialized key and value
                ((IMap) targetMap).setAsync(onlyEntry.getKey(), onlyEntry.getValue())
                         .andThen(callback);
            } else {
                ClientMessage request = MapPutAllCodec.encodeRequest(targetMap.getName(), partitionEntries.getValue());
                new ClientInvocation(client, request, targetMap.getName(), partitionId).invoke()
                                                                                       .andThen(callback);
            }
        }
        return resultFuture;
    }

    private static List<Integer> asIntegerList(int[] array) {
        return new AbstractList<Integer>() {
            @Override
            public Integer get(int index) {
                return array[index];
            }

            @Override
            public int size() {
                return array.length;
            }
        };
    }
}
