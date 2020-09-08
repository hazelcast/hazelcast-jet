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

package com.hazelcast.jet.sql;

import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Collections.singletonList;

public class SqlTest extends JetSqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void supportsValues() {
        assertEmpty(
                "SELECT a - b FROM (VALUES (1, 2)) AS t (a, b) WHERE a + b > 4"
        );

        assertRowsEventuallyInAnyOrder(
                "SELECT a - b FROM (VALUES (7, 11)) AS t (a, b) WHERE a + b > 4",
                singletonList(new Row((byte) -4))
        );
    }
}
