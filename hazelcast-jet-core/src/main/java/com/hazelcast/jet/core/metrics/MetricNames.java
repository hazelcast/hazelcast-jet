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

/**
 * Each metric provided by Jet has a specific name which conceptually identifies what it's being used to measure. Besides
 * their name metrics also have a description made up by tags, but those are more like attributes which describe a specific
 * instance of the metric and are not directly part of the identity of the metric.
 * <p>
 * Metric names are also being duplicated by the metric tag {@link MetricTags#METRIC}.
 * <p>
 * The constants described here represent the various names metrics can take in Jet.
 */
public final class MetricNames {
    //todo: soon!
}
