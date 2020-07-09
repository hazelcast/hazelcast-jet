package com.hazelcast.jet.sql;

import org.junit.Test;

import static java.util.Arrays.asList;

public class SqlTest extends SqlTestSupport {

    @Test
    public void supportsValues() {
        assertRowsEventuallyAnyOrder(
                "SELECT a - b FROM (VALUES (1, 2), (3, 5), (7, 11)) AS t (a, b) WHERE a + b > 4",
                asList(
                        new Row((byte) -2),
                        new Row((byte) -4)
                )
        );
    }
}
