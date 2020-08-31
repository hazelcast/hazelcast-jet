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
 * For proper functionality the {@code hazelcast-jet-sql.jar`} has to be on
 * the class path.
 */
public interface JetSqlService extends SqlService {
}
