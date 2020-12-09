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

import com.hazelcast.com.fasterxml.jackson.core.JsonParseException;
import com.hazelcast.com.fasterxml.jackson.core.io.JsonEOFException;
import com.hazelcast.jet.hadoop.file.model.User;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonFileFormatTest extends BaseFileFormatTest {

    @Test
    @Ignore("It's an issue, remove this @Ignore once it will be fixed")
    public void shouldReadPrettyPrintedJsonFile() throws Exception {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("pretty-printed-file-*.json")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadJsonLinesFile() throws Exception {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file.jsonl")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadJsonLinesFileWithMoreAttributesThanTargetClass() throws Exception {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-more-attributes.jsonl")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 7),
                new User("Ali", 42)
        );
    }

    @Test
    public void shouldReadJsonLinesFileWithLessColumnsThanTargetClass() throws Exception {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-less-attributes.jsonl")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source,
                new User("Frantisek", 0),
                new User("Ali", 0)
        );
    }

    @Test
    public void shouldReadEmptyJsonFile() throws Exception {

        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-empty.json")
                                                    .format(FileFormat.json(User.class));

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }

    @Test
    public void shouldThrowWhenInvalidFileType() throws Exception {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("invalid-data.png")
                                                    .format(FileFormat.json(User.class));

        assertJobFailed(source, JsonParseException.class, "Unexpected character");
    }

    @Test
    public void shouldThrowWhenWrongFormatting() throws Exception {
        FileSourceBuilder<User> source = FileSources.files(currentDir + "/src/test/resources")
                                                    .glob("file-invalid.jsonl")
                                                    .format(FileFormat.json(User.class));

        assertJobFailed(source, JsonEOFException.class, "Unexpected end-of-input");
    }
}
