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

package com.hazelcast.jet.config;

/**
 * Represents the type of the resource to be uploaded.
 */
public enum ResourceType {
    /**
     * Represents a plain file.
     */
    FILE,
    /**
     * Represents a directory of plain files.
     */
    DIRECTORY,
    /**
     * Represents a class that will be on the classpath of the Jet job.
     */
    CLASS,
    /**
     * Represents a JAR file whose classes will be on the classpath of the Jet
     * job.
     */
    JAR,
    /**
     * Represents a ZIP file that contains JAR files, all of whose classes will
     * be on the classpath of the Jet job.
     */
    JARS_IN_ZIP;

    /**
     * Returns whether this resource type represents an archive containing
     * classes.
     */
    public boolean isClassArchive() {
        return this == ResourceType.JAR || this == ResourceType.JARS_IN_ZIP;
    }

}
