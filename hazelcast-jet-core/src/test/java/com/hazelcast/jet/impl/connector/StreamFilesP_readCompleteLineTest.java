/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import org.junit.Test;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class StreamFilesP_readCompleteLineTest {

    private StreamFilesP p = new StreamFilesP("", StandardCharsets.UTF_8, 0, 0);

    @Test
    public void when_emptyFile_then_null() throws Exception {
        assertEquals(null, p.readCompleteLine(new StringReader("")));
    }

    @Test
    public void when_nonTerminatedSingleLine_then_null() throws Exception {
        assertEquals(null, p.readCompleteLine(new StringReader("blabla")));
    }

    @Test
    public void when_terminatedSingleLine_then_singleLine() throws Exception {
        StringReader reader = new StringReader("blabla\n");

        assertEquals("blabla", p.readCompleteLine(reader));
    }

    @Test
    public void when_nonTerminatedSecondLine_then_singleLine() throws Exception {
        StringReader reader = new StringReader("blabla\nbla");

        assertEquals("blabla", p.readCompleteLine(reader));
        assertEquals(null, p.readCompleteLine(reader));
    }

    @Test
    public void when_terminatedSecondLine_then_twoLines() throws Exception {
        StringReader reader = new StringReader("blabla\nbla\n");

        assertEquals("blabla", p.readCompleteLine(reader));
        assertEquals("bla", p.readCompleteLine(reader));
    }

    @Test
    public void when_emptyLine_then_emptyLine() throws Exception {
        StringReader reader = new StringReader("\nbla\n");

        assertEquals("", p.readCompleteLine(reader));
        assertEquals("bla", p.readCompleteLine(reader));
    }

    @Test
    public void when_twoEmptyLines_then_emptyLine() throws Exception {
        StringReader reader = new StringReader("\n\nbla\n");

        assertEquals("", p.readCompleteLine(reader));
        assertEquals("", p.readCompleteLine(reader));
        assertEquals("bla", p.readCompleteLine(reader));
    }

    @Test
    public void test_windowsEndLines() throws Exception {
        StringReader reader = new StringReader("blabla\r\nbla\r\n");

        assertEquals("blabla", p.readCompleteLine(reader));
        assertEquals("bla", p.readCompleteLine(reader));
    }

    @Test
    public void test_mac9EndLines() throws Exception {
        StringReader reader = new StringReader("blabla\rbla\r");

        assertEquals("blabla", p.readCompleteLine(reader));
        assertEquals("bla", p.readCompleteLine(reader));
    }
}
