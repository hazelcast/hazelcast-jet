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
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.sql.imap.IMapSqlConnector;
import com.hazelcast.jet.sql.schema.JetSchema;
import org.junit.Test;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static java.util.Arrays.asList;

public class SqlTest extends JetTestSupport {
    @Test
    public void test() {
        JetInstance jet = createJetMember();

        JetSqlService sqlService = new JetSqlService(jet);
        sqlService.getSchema().createTable("my_map", JetSchema.IMAP_LOCAL_SERVER,
                createMap(IMapSqlConnector.TO_MAP_NAME, "my_map"),
                asList(entry("id", Integer.class), entry("name", String.class)));
        sqlService.getSchema().createTable("my_map2", JetSchema.IMAP_LOCAL_SERVER,
                createMap(IMapSqlConnector.TO_MAP_NAME, "my_map2"),
                asList(entry("id", Integer.class), entry("field", String.class)));

        sqlService.parse("SELECT field FROM my_map, my_map2");
//        sqlService.parse("SELECT name FROM my_map");
    }
}
