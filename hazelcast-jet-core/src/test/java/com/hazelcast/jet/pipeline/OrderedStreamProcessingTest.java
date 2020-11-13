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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.GeneratorFunction;
import com.hazelcast.jet.pipeline.test.ParallelStreamP;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.stream.LongStream;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.core.test.JetAssert.assertFalse;
import static com.hazelcast.jet.core.test.JetAssert.assertTrue;
import static com.hazelcast.jet.core.test.JetAssert.fail;
import static java.util.stream.Collectors.toList;


@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class OrderedStreamProcessingTest extends JetTestSupport implements Serializable {

    private static final int LOCAL_PARALLELISM = 11;
    private static Pipeline p;
    private static JetInstance jet;

    @Parameter(value = 0)
    public FunctionEx<StreamStage<Long>, StreamStage<Long>> transform;

    @Parameter(value = 1)
    public String transformName;

    @Before
    public void setup() {
        jet = createJetMember();
        p = Pipeline.create();
    }

    @After
    public void after() {
        jet.shutdown();
    }

    @Parameters(name = "{index}: transform={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                createParamSet(
                        stage -> stage
                                .map(x -> x)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        "map"
                ),
                createParamSet(
                        stage -> stage
                                .flatMap(Traversers::singleton)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        "flat-map"
                ),
                createParamSet(
                        stage -> stage
                                .mapUsingReplicatedMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        "map-using-replicated-map"
                ),
                createParamSet(
                        stage -> stage
                                .mapStateful(LongAccumulator::new, (s, x) -> x)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        "map-stateful-global"
                )
                ,
                createParamSet(
                        stage -> stage
                                .mapStateful(LongAccumulator::new, (s, x) -> x)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        "map-stateful-global"
                ),
                createParamSet(
                        stage -> stage
                                .<Long>customTransform("custom-transform",
                                        Processors.mapP(FunctionEx.identity()))
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        "custom-transform"
                ),
                createParamSet(
                        stage -> stage
                                .mapUsingIMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        "map-using-imap"
                ),
                createParamSet(
                        stage -> stage
                                .filter(PredicateEx.alwaysTrue())
                                .setLocalParallelism(LOCAL_PARALLELISM),
                        "filter"
                )
        );
    }

    private static Object[] createParamSet(
            FunctionEx<StreamStage<Long>, StreamStage<Long>> transform,
            String transformName
    ) {
        return new Object[]{transform, transformName};
    }


    @Test
    public void ordered_stream_processing_test() {
        int itemCount = 250;
        int eventsPerSecondPerGenerator = 25;
        int generatorCount = 4;

        // Generate monotonic increasing items that are distinct for each generator.
        GeneratorFunction<Long> generator1 = (ts, seq) -> generatorCount * seq;
        GeneratorFunction<Long> generator2 = (ts, seq) -> generatorCount * seq + 1;
        GeneratorFunction<Long> generator3 = (ts, seq) -> generatorCount * seq + 2;
        GeneratorFunction<Long> generator4 = (ts, seq) -> generatorCount * seq + 3;

        StreamStage<Long> srcStage = p.readFrom(itemsParallel(
                eventsPerSecondPerGenerator,
                Arrays.asList(generator1, generator2, generator3, generator4))
        ).withIngestionTimestamps();

        StreamStage<Long> applied = srcStage.apply(transform);

        applied.mapStateful(() -> create(generatorCount), this::orderValidator)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> {
                            assertTrue("when", itemCount <= list.size());
                            assertFalse("There is some reordered items in the list", list.contains(false));
                        }
                ));

        p.setPreserveOrder(true);
        Job job = jet.newJob(p);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    @Test
    public void ordered_stream_processing_test2() {
        int eventsPerSecondPerGenerator = 30;
        int generatorCount = 4;
        int itemCount = 200;

        // Generate monotonic increasing items that are distinct for each generator.
        GeneratorFunction<Long> generator1 = (ts, seq) -> generatorCount * seq;
        GeneratorFunction<Long> generator2 = (ts, seq) -> generatorCount * seq + 1;
        GeneratorFunction<Long> generator3 = (ts, seq) -> generatorCount * seq + 2;
        GeneratorFunction<Long> generator4 = (ts, seq) -> generatorCount * seq + 3;

        List<Long> sequence1 = LongStream.range(0, itemCount).map(i -> generatorCount * i).boxed().collect(toList());
        List<Long> sequence2 = LongStream.range(0, itemCount).map(i -> generatorCount * i + 1).boxed().collect(toList());
        List<Long> sequence3 = LongStream.range(0, itemCount).map(i -> generatorCount * i + 2).boxed().collect(toList());
        List<Long> sequence4 = LongStream.range(0, itemCount).map(i -> generatorCount * i + 3).boxed().collect(toList());

        StreamStage<Long> srcStage = p.readFrom(itemsParallel(
                eventsPerSecondPerGenerator,
                Arrays.asList(generator1, generator2, generator3, generator4))
        ).withIngestionTimestamps();

        StreamStage<Long> applied = srcStage.apply(transform);

        applied.filter(i -> i % generatorCount == 0)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence1.toArray())));

        applied.filter(i -> i % generatorCount == 1)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence2.toArray())));
        applied.filter(i -> i % generatorCount == 2)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence3.toArray())));
        applied.filter(i -> i % generatorCount == 3)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence4.toArray())));

        p.setPreserveOrder(true);

        Job job = jet.newJob(p);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }


    /**
     * Returns a streaming source that generates events created by the {@code
     * generatorFns} at the specified rate in each of its parallel branches.
     * In the default use of this source, a source processor is created for
     * each generator function and these processors generate events with
     * these assigned generator functions. If total parallelism is more than
     * the number of generatorFns, some source processors will remain idle.
     * This source's preferred local parallelism is determined as if it would
     * work in a single member.
     * <p>
     * The source supports {@linkplain
     * StreamSourceStage#withNativeTimestamps(long) native timestamps}. The
     * timestamp is the current system time at the moment they are generated.
     * The source is not distributed and all the items are generated on the
     * same node. This source is not fault-tolerant. The sequence will be
     * reset once a job is restarted.
     *
     * @since 4.4
     */
    @Nonnull
    private static <T> StreamSource<T> itemsParallel(
            long eventsPerSecondPerGenerator, @Nonnull List<? extends GeneratorFunction<T>> generatorFns
    ) {
        Objects.requireNonNull(generatorFns, "generatorFns");

        return Sources.streamFromProcessorWithWatermarks("itemsParallel",
                true,
                eventTimePolicy -> ProcessorMetaSupplier.of(generatorFns.size(), () ->
                        new ParallelStreamP<>(eventsPerSecondPerGenerator, eventTimePolicy, generatorFns)));
    }


    private LongAccumulator[] create(int generatorCount) {
        LongAccumulator[] state = new LongAccumulator[generatorCount];
        for (int i = 0; i < generatorCount; i++) {
            state[i] = new LongAccumulator(Long.MIN_VALUE);
        }
        return state;
    }

    private boolean orderValidator(LongAccumulator[] s, Long eventValue) {
        LongAccumulator acc = s[eventValue.intValue() % s.length];
        if (acc.get() >= eventValue) {
            return false;
        } else {
            acc.set(eventValue);
            return true;
        }
    }
}