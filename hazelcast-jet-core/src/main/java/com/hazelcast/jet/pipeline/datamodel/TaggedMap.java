/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.datamodel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A heterogeneous map, mapping a {@code Tag<E>} to a value of type {@code
 * E}. Each mapping can have a different {@code E}.
 * <p>
 * A tagged map is a less typesafe, but more flexible alternative to a
 * tuple. The tuple has a fixed number of integer-indexed, statically typed
 * fields, and a tagged map has a variable number of tag-indexed fields
 * whose whose static type is encoded in the tags.
 */
public class TaggedMap implements Serializable {
    private final Map<Tag, Object> map = new HashMap<>();

    @SuppressWarnings("unchecked")
    public <E> E get(Tag<E> tag) {
        return (E) map.get(tag);
    }

    public <E> void put(Tag<E> tag, E value) {
        map.put(tag, value);
    }
}
