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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Processor.Context;
import com.hazelcast.util.MutableLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Distributed.Function.identity;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;
import static java.util.Arrays.asList;
import static java.util.Collections.shuffle;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SlidingWindowPTest extends StreamingTestSupport {

    @Parameter
    public boolean hasDeduct;

    @Parameter(1)
    public  boolean isMutableFrame;

    @Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {true, true},
                {true, false},
                {false, true},
                {false, false}
        });
    }

    private SlidingWindowP<Object, ?, Long> processor;

    @Before
    public void before() {
        WindowDefinition windowDef = new WindowDefinition(1, 0, 4);
        WindowOperation<Object, ?, Long> operation;

        if (isMutableFrame) {
            operation = WindowOperation.<Object, MutableLong, Long>of(
                    MutableLong::new,
                    (acc, val) -> {
                        throw new UnsupportedOperationException();
                    },
                    (acc1, acc2) -> {
                        acc1.value += acc2.value;
                        return acc1;
                    },
                    hasDeduct ? (acc1, acc2) -> {
                        acc1.value -= acc2.value;
                        return acc1;
                    } : null,
                    acc -> acc.value);
        } else {
            operation = WindowOperation.of(
                    () -> 0L,
                    (acc, val) -> {
                        throw new UnsupportedOperationException();
                    },
                    (acc1, acc2) -> acc1 + acc2,
                    hasDeduct ? (acc1, acc2) -> (Long) acc1 - acc2 : null,
                    identity());
        }

        processor = slidingWindow(windowDef, operation, true).get();
        processor.init(outbox, mock(Context.class));
    }

    @After
    public void after() {
        assertTrue("seqToKeyToFrame is not empty: " + processor.seqToKeyToFrame, processor.seqToKeyToFrame.isEmpty());
        assertTrue("slidingWindow is not empty: " + processor.slidingWindow, processor.slidingWindow.isEmpty());
    }

    @Test
    public void when_noFramesReceived_then_onlyEmitPunc() {
        // Given
        inbox.addAll(asList(
                punc(1)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                punc(1)
        ));
    }

    @Test
    public void when_receiveAscendingSeqs_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                frame(0, 1),
                frame(1, 1),
                frame(2, 1),
                frame(3, 1),
                frame(4, 1),
                punc(1),
                punc(2),
                punc(3),
                punc(4),
                punc(5),
                punc(6),
                punc(7)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                outboxFrame(0, 1),
                outboxFrame(1, 2),
                punc(1),
                outboxFrame(2, 3),
                punc(2),
                outboxFrame(3, 4),
                punc(3),
                outboxFrame(4, 4),
                punc(4),
                outboxFrame(5, 3),
                punc(5),
                outboxFrame(6, 2),
                punc(6),
                outboxFrame(7, 1),
                punc(7)
        ));
    }

    @Test
    public void when_receiveDescendingSeqs_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                frame(4, 1),
                frame(3, 1),
                frame(2, 1),
                frame(1, 1),
                frame(0, 1),
                punc(1),
                punc(2),
                punc(3),
                punc(4),
                punc(5),
                punc(6),
                punc(7)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                outboxFrame(0, 1),
                outboxFrame(1, 2),
                punc(1),
                outboxFrame(2, 3),
                punc(2),
                outboxFrame(3, 4),
                punc(3),
                outboxFrame(4, 4),
                punc(4),
                outboxFrame(5, 3),
                punc(5),
                outboxFrame(6, 2),
                punc(6),
                outboxFrame(7, 1),
                punc(7)
        ));
    }

    @Test
    public void when_receiveRandomSeqs_then_emitAscending() {
        // Given
        final List<Long> frameSeqsToAdd = LongStream.range(0, 100).boxed().collect(toList());
        shuffle(frameSeqsToAdd);
        for (long seq : frameSeqsToAdd) {
            inbox.add(frame(seq, 1));
        }
        for (long i = 1; i <= 105; i++) {
            inbox.add(punc(i));
        }

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        List<Object> expectedOutbox = new ArrayList<>();
        expectedOutbox.addAll(Arrays.asList(
                outboxFrame(0, 1),
                outboxFrame(1, 2),
                punc(1),
                outboxFrame(2, 3),
                punc(2),
                outboxFrame(3, 4),
                punc(3)
        ));
        for (long seq = 4; seq < 100; seq++) {
            expectedOutbox.add(outboxFrame(seq, 4));
            expectedOutbox.add(punc(seq));
        }
        expectedOutbox.addAll(Arrays.asList(
                outboxFrame(100, 3),
                punc(100),
                outboxFrame(101, 2),
                punc(101),
                outboxFrame(102, 1),
                punc(102),
                punc(103),
                punc(104),
                punc(105)
        ));

        assertOutbox(expectedOutbox);
    }

    @Test
    public void when_receiveWithGaps_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                frame(0, 1),
                frame(10, 1),
                frame(11, 1),
                punc(15),
                frame(16, 3),
                punc(19)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                outboxFrame(0, 1),
                outboxFrame(1, 1),
                outboxFrame(2, 1),
                outboxFrame(3, 1),

                outboxFrame(10, 1),
                outboxFrame(11, 2),
                outboxFrame(12, 2),
                outboxFrame(13, 2),
                outboxFrame(14, 1),

                punc(15),
                outboxFrame(16, 3),
                outboxFrame(17, 3),
                outboxFrame(18, 3),
                outboxFrame(19, 3),
                punc(19)
        ));
    }

    @Test
    public void when_receiveWithGaps_then_doNotSkipFrames() {
        // Given
        inbox.addAll(asList(
                frame(10, 1),
                frame(11, 1),
                frame(12, 1),
                punc(1),
                frame(2, 1),
                frame(3, 1),
                frame(4, 1),
                punc(4),
                punc(12),
                punc(15)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                punc(1),
                outboxFrame(2, 1),
                outboxFrame(3, 2),
                outboxFrame(4, 3),
                punc(4),
                outboxFrame(5, 3),
                outboxFrame(6, 2),
                outboxFrame(7, 1),
                outboxFrame(10, 1),

                outboxFrame(11, 2),
                outboxFrame(12, 3),
                punc(12),
                outboxFrame(13, 3),
                outboxFrame(14, 2),
                outboxFrame(15, 1),
                punc(15)
        ));
    }

    private Frame<Long, ?> frame(long seq, long value) {
        return new Frame<>(seq, 77L, isMutableFrame ? MutableLong.valueOf(value) : value);
    }

    private Frame<Long, ?> outboxFrame(long seq, long value) {
        return new Frame<>(seq, 77L, value);
    }
}
