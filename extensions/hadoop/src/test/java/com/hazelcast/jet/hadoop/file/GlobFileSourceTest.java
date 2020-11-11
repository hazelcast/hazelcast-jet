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

import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class GlobFileSourceTest extends BaseFileFormatTest {

    @Test
    public void shouldReadFilesMatchingGlob() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/glob/file*")
                                                      .withFormat(FileFormat.text());

        assertItemsInSource(source, "file", "file1");
    }

    @Test
    public void shouldReadFilesMatchingGlobInTheMiddle() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/glob/f*le")
                                                      .withFormat(FileFormat.text());

        assertItemsInSource(source, "file");
    }

    @Test
    public void shouldReadFilesMatchingGlobInPath() {
        assumeThat(useHadoop).isTrue();
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/*/file")
                                                      .withFormat(FileFormat.text());

        assertItemsInSource(source, "file");
    }

    @Test
    @Ignore("windows don't support * in filenames, either remove this test or make it non-windows only")
    public void shouldReadFileWithEscapedGlob() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/glob/file\\*")
                                                      .withFormat(FileFormat.text());

        assertItemsInSource(source, "file*");
    }

    @Test
    public void shouldReadAllFilesInDirectory() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/directory/")
                                                      .withFormat(FileFormat.text());

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(2));
    }

    @Test
    public void shouldReadAllFilesInDirectoryNoSlash() {
        assumeThat(useHadoop).isTrue();
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/directory")
                                                      .withFormat(FileFormat.text());

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(2));
    }
}
