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
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.sql.imap.IMapSqlConnector;
import com.hazelcast.jet.sql.schema.JetSchema;
import com.hazelcast.map.IMap;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class SqlTest extends JetTestSupport {
    @Test
    public void test() throws Exception {
        JetInstance jet = createJetMember();

        JetSqlService sqlService = new JetSqlService(jet);
        sqlService.getSchema().createTable("my_map", JetSchema.IMAP_LOCAL_SERVER,
                createMap(IMapSqlConnector.TO_MAP_NAME, "my_map"),
                asList(entry("__key", Integer.class), entry("name", String.class)));
//        sqlService.getSchema().createTable("my_map2", JetSchema.IMAP_LOCAL_SERVER,
//                createMap(IMapSqlConnector.TO_MAP_NAME, "my_map2"),
//                asList(entry("__key", Integer.class), entry("field", String.class)));

        IMap<Integer, String> map = jet.getMap("my_map");
        map.put(0, "value-0");
        map.put(1, "value-1");
        map.put(2, "value-2");

        Observable<Object[]> observable = sqlService.execute("SELECT name FROM my_map WHERE __key=10");
        List<Object[]> result = observable.toFuture(str -> str.collect(toList())).get();

        assertEquals(asList(new Object[]{"value-0"}, new Object[]{"value-1"}, new Object[]{"value-2"}), result);
    }
}
