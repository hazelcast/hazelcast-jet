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

package com.hazelcast.jet;

import com.hazelcast.sql.SqlService;

/**
 * The Hazelcast Jet SQL service.
 * <p>
 * The service is in beta state. Behavior and API might change in future
 * releases. Binary compatibility is not guaranteed between minor or patch
 * releases.
 * <p>
 * Hazelcast can execute SQL statements using either the default SQL
 * backend contained in the Hazelcast IMDG code, or using the Jet SQL
 * backend in this package. The algorithm is this: we first try the
 * default backend, if it can't execute a particular statement, we try the
 * Jet backend.
 * <p>
 * For proper functionality the {@code hazelcast-jet-sql.jar} has to be on
 * the class path.
 * <p>
 * The text below summarizes Hazelcast Jet SQL features. For a summary of
 * the default SQL engine features, see the {@linkplain SqlService
 * superclass} documentation.
 *
 * <h1>Overview</h1>
 *
 * Hazelcast Jet is able to execute distributed SQL statements over any Jet
 * connector that supports the SQL integration. Currently those are:
 *
 * <ul>
 *     <li>local IMaps
 *     <li>Apache Kafka topics
 *     <li>Files (local and remote)
 * </ul>
 *
 * Each connector specifies its own serialization formats and a way of
 * mapping the stored objects to records with column names and SQL types.
 * See the individual connectors for details.
 * <p>
 * In the first release we support a very limited set of features,
 * essentially only reading and writing from/to the above connectors and
 * projection + filtering. Currently these are unsupported: joins,
 * grouping, aggregation. We plan to support these in the future.
 *
 * <h1>Full Documentation</h1>
 *
 * The full documentation of all SQL features is available at <a
 * href='https://jet-start.sh/docs/sql/'>https://jet-start.sh/docs/sql/</a>.
 */
public interface JetSqlService extends SqlService {
}
