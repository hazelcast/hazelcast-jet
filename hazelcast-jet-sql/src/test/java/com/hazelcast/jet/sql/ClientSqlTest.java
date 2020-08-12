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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ClientSqlTest extends JetSqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void basicTest() {
        JetInstance client = factory().newClient();
        SqlService sqlService = client.getHazelcastInstance().getSql();
        Map<Integer, Integer> myMap = client.getMap("my_map");
        myMap.put(1, 2);

        SqlResult result = sqlService.query("select /*+jet*/ __key, this from my_map");
        Iterator<SqlRow> iterator = result.iterator();
        SqlRow row = iterator.next();
        assertEquals(Integer.valueOf(1), row.getObject(0));
        assertEquals(Integer.valueOf(2), row.getObject(1));
        assertFalse(iterator.hasNext());
    }
}
