package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PipelineTest {

    @Test
    public void isEmpty() {
        // given
        Pipeline p1 = Pipeline.create();
        Pipeline p2 = Pipeline.create();

        p2.readFrom(TestSources.items(1, 2, 3));

        // when
        boolean p1Empty = p1.isEmpty();
        boolean p2Empty = p2.isEmpty();

        // then
        assertThat(p1Empty).isTrue();
        assertThat(p2Empty).isFalse();
    }

}