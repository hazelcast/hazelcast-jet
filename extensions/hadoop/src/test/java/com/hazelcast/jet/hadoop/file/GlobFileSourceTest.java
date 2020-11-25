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
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class GlobFileSourceTest extends BaseFileFormatTest {

    @Test
    public void shouldReadFilesMatchingGlob() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/glob/file*")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "file", "file1");
    }

    @Test
    public void shouldNotReadAnyFile() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/glob/fi")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, items -> assertThat(items).isEmpty());
    }

    @Test
    public void shouldReadFilesMatchingGlobInTheMiddle() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/glob/f*le")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "file");
    }

    @Test
    public void shouldReadFilesMatchingGlobInPath() {
        assumeThat(useHadoop).isTrue();
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/*/file")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "file");
    }

    @Test
    public void shouldReadFileWithEscapedGlob() throws IOException {
        assumeThatNoWindowsOS(); // * is not allowed in filename

        try (PrintWriter out = new PrintWriter("target/file*")) {
            out.print("file*");
        }

        FileSourceBuilder<String> source = FileSources.files("target/file\\*")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "file*");

        source = FileSources.files("target/file*")
                            .format(FileFormat.text());

        assertItemsInSource(source, "file*");
    }

    @Test
    public void shouldReadAllFilesInDirectory() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/directory/")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(2));
    }

    @Test
    public void shouldReadAllFilesInDirectoryNoSlash() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/directory")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(2));
    }

    @Test
    public void shouldReadAllFilesInDirectoryWithNativeSeparator() {
        String path = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "directory";
        FileSourceBuilder<String> source = FileSources.files(path)
                                                      .format(FileFormat.text());

        assertItemsInSource(source, (collected) -> assertThat(collected).hasSize(2));
    }

    @Test
    public void shouldIgnoreSubdirectories() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/level1")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "level1_file");
    }

    @Test
    public void shouldIgnoreSubdirectoriesWhenUsingGlob() {
        assumeThat(useHadoop).isFalse();
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/level1/*")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, "level1_file");
    }

    @Test
    public void shouldReadPathNoDirectoryFileOnly() {
        FileSourceBuilder<String> source = FileSources.files("pom.xml")
                                                      .format(FileFormat.text());

        assertItemsInSource(source, (collected) ->
                assertThat(collected).anyMatch(s -> s.contains("<artifactId>hazelcast-jet-hadoop</artifactId>"))
        );
    }
}
