package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.test.TestSupport;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.Traversers.traverseStream;
import static java.util.Arrays.asList;

public class TransformBatchedPTest extends JetTestSupport {

    @Test
    public void test_process() {
        TestSupport
                .verifyProcessor(() -> new TransformBatchedP<>(
                        (List<Integer> items) -> traverseStream(items.stream().map(i -> i * 2))
                ))
                .input(asList(1, 2, 3, 4, 5))
                .expectOutput(asList(2, 4, 6, 8, 10));
    }
}