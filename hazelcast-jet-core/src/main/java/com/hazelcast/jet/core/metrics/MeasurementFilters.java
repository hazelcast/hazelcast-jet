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

package com.hazelcast.jet.core.metrics;

import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Static utility class for creating various {@link Measurement} filtering predicates.
 */
public final class MeasurementFilters {

    private MeasurementFilters() { }

    /**
     * Matches a {@link Measurement} which contain the specified tag.
     *
     * @param tag the tag of interest
     * @return a filtering predicate
     */
    public static Predicate<Measurement> containsTag(String tag) {
        return new IsPresent(tag);
    }

    /**
     * Matches a {@link Measurement} which contains the specified tag and the tag has the specified value.
     *
     * @param tag   the tag to match
     * @param value the value the tag has to have
     * @return a filtering predicate
     */
    public static Predicate<Measurement> tagValueEquals(String tag, String value) {
        return new Equals(tag, value);
    }

    /**
     * Matches a {@link Measurement} which has this exact tag with a value matching the provided regular expression.
     *
     * @param tag         the tag to match
     * @param valueRegexp regular expression to match the value against
     * @return a filtering predicate
     */
    public static Predicate<Measurement> tagValueMatches(String tag, String valueRegexp) {
        return new Matches(tag, valueRegexp);
    }

    private static class IsPresent implements Predicate<Measurement> {

        private final String tag;

        IsPresent(String tag) {
            this.tag = tag;
        }

        @Override
        public boolean test(Measurement measurement) {
            return measurement.getTag(tag) != null;
        }

    }

    private static class Equals implements Predicate<Measurement> {

        private final String tag;
        private final String value;

        Equals(String tag, String value) {
            this.tag = tag;
            this.value = value;
        }

        @Override
        public boolean test(Measurement measurement) {
            return value.equals(measurement.getTag(tag));
        }

    }

    private static class Matches implements Predicate<Measurement> {

        private final String tag;
        private final Pattern valueRegexp;

        Matches(String tag, String valueRegexp) {
            this.tag = tag;
            this.valueRegexp = Pattern.compile(valueRegexp);
        }

        @Override
        public boolean test(Measurement measurement) {
            String value = measurement.getTag(tag);
            return value == null || valueRegexp.matcher(value).matches();
        }

    }
}
