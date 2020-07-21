/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.test.IgnoreOnJenkinsWindows;
import org.junit.Before;
import org.junit.experimental.categories.Category;

@Category({IgnoreOnJenkinsWindows.class})
public abstract class HadoopTestSupport extends SimpleTestInClusterSupport {

    @Before
    public void hadoopSupportBefore() {
        // Tests fail on windows. If you want to run them, comment out this line and
        // follow this instructions: https://stackoverflow.com/a/35652866/952135
        assumeThatNoWindowsOS();
    }
}
