/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.lang.Math.abs;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.IntStream.range;

public final class Util {

    private static final DateTimeFormatter LOCAL_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final Pattern TRAILING_NUMBER_PATTERN = Pattern.compile("(.*)-([0-9]+)");

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

    public static void uncheckRun(@Nonnull RunnableEx r) {
        r.run();
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

    public static JetInstance getJetInstance(NodeEngine nodeEngine) {
        return nodeEngine.<JetService>getService(JetService.SERVICE_NAME).getJetInstance();
    }

    public static long addClamped(long a, long b) {
        long sum = a + b;
        return sumHadOverflow(a, b, sum)
                ? (a >= 0 ? Long.MAX_VALUE : Long.MIN_VALUE)
                : sum;
    }

    /**
     * Calculates {@code a - b}, returns {@code Long.MAX_VALUE} or {@code
     * Long.MIN_VALUE} if the result would overflow.
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
    private static boolean diffHadOverflow(long a, long b, long diff) {
        return ((a ^ b) & (a ^ diff)) < 0;
    }

    /**
     * Checks that the {@code object} implements {@link Serializable} or {@link
     * DataSerializable}. In case of {@code Serializable}, it additionally
     * checks it is correctly serializable by actually trying to serialize it.
     * This will reveal a non-serializable field early.
     *
     * @param object     object to check
     * @param objectName object description for the exception
     * @return given object
     * @throws IllegalArgumentException if {@code object} is not serializable
     */
    @Nullable
    public static <T> T checkSerializable(@Nullable T object, @Nonnull String objectName) {
        if (object == null) {
            return null;
        }
        if (object instanceof DataSerializable) {
            // hz-serialization is implemented, but we cannot actually check it - we don't have a
            // SerializationService at hand.
            return object;
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
        return object;
    }

    /**
     * Checks that the {@code object} is not null and implements
     * {@link Serializable} and is correctly serializable by actually
     * trying to serialize it. This will reveal some non-serializable
     * field early.
     * <p>
     * Usage:
     * <pre>{@code
     * void setValue(@Nonnull Object value) {
     *     this.value = checkNonNullAndSerializable(value, "value");
     * }
     * }</pre>
     *
     * @param object     object to check
     * @param objectName object description for the exception
     * @return given object
     * @throws IllegalArgumentException if {@code object} is not serializable
     */
    @Nonnull
    public static <T> T checkNonNullAndSerializable(@Nonnull T object, @Nonnull String objectName) {
        //noinspection ConstantConditions
        if (object == null) {
            throw new IllegalArgumentException('"' + objectName + "\" must not be null");
        }
        checkSerializable(object, objectName);
        return object;
    }

    /**
     * Distributes the {@code objects} to {@code count} processors in a
     * round-robin fashion. If the object count is smaller than processor
     * count, an empty list is put for the rest of the processors.
     *
     * @param count   count of processors
     * @param objects list of objects to distribute
     * @return a map which has the processor index as the key and a list of objects as
     * the value
     */
    public static <T> Map<Integer, List<T>> distributeObjects(int count, List<T> objects) {
        Map<Integer, List<T>> processorToObjects = range(0, objects.size())
                .mapToObj(i -> entry(i, objects.get(i)))
                .collect(groupingBy(e -> e.getKey() % count, mapping(Map.Entry::getValue, Collectors.toList())));

        for (int i = 0; i < count; i++) {
            processorToObjects.putIfAbsent(i, emptyList());
        }
        return processorToObjects;
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
     * @param count       total number of subsets
     * @param index       index of the requested subset
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

    public static String jobNameAndExecutionId(String jobName, long executionId) {
        return "job '" + jobName + "', execution " + idToString(executionId);
    }

    public static String jobIdAndExecutionId(long jobId, long executionId) {
        return "job " + idToString(jobId) + ", execution " + idToString(executionId);
    }

    private static ZonedDateTime toZonedDateTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());
    }

    public static LocalDateTime toLocalDateTime(long timestamp) {
        return toZonedDateTime(timestamp).toLocalDateTime();
    }

    public static String toLocalTime(long timestamp) {
        return toZonedDateTime(timestamp).toLocalTime().format(LOCAL_TIME_FORMATTER);
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

    public static void lazyIncrement(AtomicLongArray counters, int index) {
        lazyAdd(counters, index, 1);
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

    @SuppressWarnings("WeakerAccess")  // used in jet-enterprise
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

    public static String sanitizeLoggerNamePart(String name) {
        return name.replace('.', '_');
    }

    public static void doWithClassLoader(ClassLoader cl, RunnableEx action) {
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
     * Returns the lower of the given guarantees.
     */
    public static ProcessingGuarantee min(ProcessingGuarantee g1, ProcessingGuarantee g2) {
        return g1.ordinal() < g2.ordinal() ? g1 : g2;
    }

    /**
     * Returns elements of the given {@code coll} in a new {@code List}, mapped
     * using the given {@code mapFn}.
     */
    @Nonnull
    public static <T, R> List<R> toList(@Nonnull Collection<T> coll, @Nonnull Function<? super T, ? extends R> mapFn) {
        return coll.stream().map(mapFn).collect(Collectors.toList());
    }
}
