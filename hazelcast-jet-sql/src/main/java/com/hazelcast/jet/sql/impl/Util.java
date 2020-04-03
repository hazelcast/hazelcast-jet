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

package com.hazelcast.jet.sql.impl;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Util {

    public static String getRequiredTableOption(Map<String, String> options, String optionName) {
        return requireNonNull(options.get(optionName), "Missing required table option: " + optionName);
    }

    public static String getRequiredServerOption(Map<String, String> options, String optionName) {
        return requireNonNull(options.get(optionName), "Missing required server option: " + optionName);
    }
}
