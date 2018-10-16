/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

/**
 * A concurrent, in-memory concurrent list implementation.
 * <p>
 * In comparison to {@link IMapJet}, the entries of the list are only stored on a
 * single partition, and as such adding more nodes will not add more storage capacity
 * to the list. While {@link IMap} doesn't provide any ordering, the items in
 * {@code IListJet} are ordered.
 * <p>
 * It's possible to use the link as a data source or sink in a Jet {@link Pipeline},
 * using {@link Sources#list(String)} or {@link Sinks#list(String)}.
 *
 * @param <E> the type of values maintained by this list
 *
 * @see IList
 * @see Sources#list(String)
 * @see Sinks#list(String) (String)
 */
public interface IListJet<E> extends IList<E> {

}
