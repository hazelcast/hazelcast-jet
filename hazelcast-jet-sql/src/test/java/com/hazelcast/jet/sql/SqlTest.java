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

import java.util.Map;

public class SqlTest extends SqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void when_submitsQuery_then_successfullyExecutesIt() {
        String name = "map";

        Map<Integer, String> map = instance().getMap(name);
        map.put(1, "Alice");
        map.put(2, "Bob");

        assertRowsInAnyOrder(
                "SELECT * FROM " + name,
                new Row(1, "Alice"),
                new Row(2, "Bob")
        );
    }
}
