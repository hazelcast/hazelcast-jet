/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.function;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * Factory methods for several common distributed functions.
 */
public final class DistributedFunctions {

    private DistributedFunctions() {
    }

    /**
     * Synonym for {@link DistributedFunction#identity}, to be used as a
     * projection function (e.g., key extractor).
     */
    @Nonnull
    public static <T> DistributedFunction<T, T> wholeItem() {
        return DistributedFunction.identity();
    }

    /**
     * Returns a function that extracts the key of a {@link Map.Entry}.
     *
     * @param <K> type of entry's key
     */
    @Nonnull
    public static <K, V> DistributedFunction<Entry<K, V>, K> entryKey() {
        return Map.Entry::getKey;
    }

    /**
     * Returns a function that extracts the value of a {@link Map.Entry}.
     *
     * @param <V> type of entry's value
     */
    @Nonnull
    public static <K, V> DistributedFunction<Entry<K, V>, V> entryValue() {
        return Map.Entry::getValue;
    }

    /**
     * Returns a function that always evaluates to the same value. This is
     * useful as a key extractor in group-by operations where classification
     * into a single group is desired.
     * <p>
     * Each invocation of this method will return a new function that will
     * return a constant, randomly generated key to improve partition balance.
     * The key has the form {@code "KEY:<3 random characters>"}
     */
    @Nonnull
    public static <T> DistributedFunction<T, String> constantKey() {
        // we use a string of 3 random characters from the range '0' .. 'z'
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String key = "KEY:" + Stream.generate(() -> String.valueOf((char) ('0' + random.nextInt('z' - '0'))))
                               .limit(3)
                               .collect(joining());
        return t -> key;
    }
}
