package com.hazelcast.jet.sql;

import org.junit.Ignore;
import org.junit.Test;

import static java.util.Arrays.asList;

public class SqlTest extends SqlTestSupport {

    @Test
    @Ignore // TODO fix it
    public void supportsValues() {
        assertRowsEventuallyAnyOrder(
                "SELECT a - b FROM (VALUES (1, 2), (3, 5), (7, 11)) AS t (a, b) WHERE a + b > 4",
                asList(
                        new Row(-2),
                        new Row(-4)
                )
        );
    }
}
