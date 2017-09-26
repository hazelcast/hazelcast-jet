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

package com.hazelcast.jet.datamodel;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A heterogeneous map from {@code Tag<E>} to {@code E}, where {@code E}
 * can be different for each tag.
 * <p>
 * This is a less typesafe, but more flexible alternative to a tuple. The
 * tuple has a fixed number of integer-indexed, statically-typed fields,
 * and {@code ItemsByTag} has a variable number of tag-indexed fields whose
 * whose static type is encoded in the tags.
 */
public class ItemsByTag implements Serializable {
    private final Map<Tag, Object> map = new HashMap<>();

    /**
     * Retrieves the object associated with the supplied tag and throws an
     * exception if there is none. The argument must not be {@code null}.
     *
     * @throws IllegalArgumentException if there is no item registered under the supplied tag
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <E> E get(@Nonnull Tag<E> tag) {
        Object got = map.get(tag);
        if (got == null) {
            throw new IllegalArgumentException("No item registered under " + tag);
        }
        return (E) got;
    }

    /**
     * Associates the supplied object with the supplied tag. Neither the tag
     * nor object may be {@code null}.
     */
    public <E> void put(@Nonnull Tag<E> tag, @Nonnull E value) {
        map.put(tag, value);
    }
}
