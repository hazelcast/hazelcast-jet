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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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

    private final boolean hasDeduct;
    private SlidingWindowP<Object, Long, Long> processor;

    @Parameters
    public static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    public SlidingWindowPTest(boolean hasDeduct) {
        this.hasDeduct = hasDeduct;
    }

    @Before
    public void before() {
        WindowDefinition windowDef = new WindowDefinition(1, 0, 4);
        WindowOperation<Object, Long, Long> operation = WindowOperation.of(
                () -> 0L,
                (acc, val) -> {
                    throw new UnsupportedOperationException();
                },
                (acc1, acc2) -> acc1 + acc2,
                hasDeduct ? (acc1, acc2) -> (Long) acc1 - acc2 : null,
                identity());
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
                frame(0, 1),
                punc(0),
                frame(1, 2),
                punc(1),
                frame(2, 3),
                punc(2),
                frame(3, 4),
                punc(3),
                frame(4, 4),
                punc(4),
                frame(5, 3),
                punc(5),
                frame(6, 2),
                punc(6),
                frame(7, 1),
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
                frame(0, 1),
                punc(0),
                frame(1, 2),
                punc(1),
                frame(2, 3),
                punc(2),
                frame(3, 4),
                punc(3),
                frame(4, 4),
                punc(4),
                frame(5, 3),
                punc(5),
                frame(6, 2),
                punc(6),
                frame(7, 1),
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
        expectedOutbox.add(frame(0, 1));
        expectedOutbox.add(punc(0));
        expectedOutbox.add(frame(1, 2));
        expectedOutbox.add(punc(1));
        expectedOutbox.add(frame(2, 3));
        expectedOutbox.add(punc(2));
        expectedOutbox.add(frame(3, 4));
        expectedOutbox.add(punc(3));
        for (long seq = 4; seq < 100; seq++) {
            expectedOutbox.add(frame(seq, 4));
            expectedOutbox.add(punc(seq));
        }
        expectedOutbox.add(frame(100, 3));
        expectedOutbox.add(punc(100));
        expectedOutbox.add(frame(101, 2));
        expectedOutbox.add(punc(101));
        expectedOutbox.add(frame(102, 1));
        expectedOutbox.add(punc(102));
        expectedOutbox.add(punc(103));
        expectedOutbox.add(punc(104));
        expectedOutbox.add(punc(105));

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
                frame(0, 1),
                punc(0),
                frame(1, 1),
                punc(1),
                frame(2, 1),
                punc(2),
                frame(3, 1),
                punc(3),

                frame(10, 1),
                punc(10),
                frame(11, 2),
                punc(11),
                frame(12, 2),
                punc(12),
                frame(13, 2),
                punc(13),
                frame(14, 1),
                punc(14),

                punc(15),
                frame(16, 3),
                punc(16),
                frame(17, 3),
                punc(17),
                frame(18, 3),
                punc(18),
                frame(19, 3),
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
                frame(2, 1),
                punc(2),
                frame(3, 2),
                punc(3),
                frame(4, 3),
                punc(4),
                frame(5, 3),
                punc(5),
                frame(6, 2),
                punc(6),
                frame(7, 1),
                punc(7),
                frame(10, 1),
                punc(10),

                frame(11, 2),
                punc(11),
                frame(12, 3),
                punc(12),
                frame(13, 3),
                punc(13),
                frame(14, 2),
                punc(14),
                frame(15, 1),
                punc(15)
        ));
    }

    private static Frame<Long, Long> frame(long seq, long value) {
        return new Frame<>(seq, 77L, value);
    }
}
