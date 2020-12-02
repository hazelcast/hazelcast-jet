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

package com.hazelcast.jet.hadoop.file;

import com.fasterxml.jackson.jr.stree.JrsNumber;
import com.fasterxml.jackson.jr.stree.JrsObject;
import com.fasterxml.jackson.jr.stree.JrsString;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonFileFormatTest extends BaseFileFormatTest {

    @Test
    public void shouldReadJsonLinesFile() {
        FileSourceBuilder<JrsObject> source = FileSources.files(currentDir + "/src/test/resources")
                                                         .glob("file.jsonl")
                                                         .format(FileFormat.json());

        assertItemsInSource(source,
                collected -> assertThat(collected).usingRecursiveFieldByFieldElementComparator()
                                                  .containsOnly(
                                                          new JrsObject(ImmutableMap.of(
                                                                  "name", new JrsString("Frantisek"),
                                                                  "favoriteNumber", new JrsNumber(7))
                                                          ),
                                                          new JrsObject(ImmutableMap.of(
                                                                  "name", new JrsString("Ali"),
                                                                  "favoriteNumber", new JrsNumber(42))
                                                          )
                                                  )
        );
    }

    @Test
    public void shouldReadJsonLinesFileToObject() {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file.jsonl")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }
}
