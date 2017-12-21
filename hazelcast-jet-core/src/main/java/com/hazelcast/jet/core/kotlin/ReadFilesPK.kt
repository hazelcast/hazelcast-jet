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

package com.hazelcast.jet.core.kotlin

import com.hazelcast.jet.core.CloseableProcessorSupplier
import com.hazelcast.jet.core.Processor
import com.hazelcast.jet.core.ProcessorMetaSupplier
import com.hazelcast.jet.impl.util.LoggingUtil.logFinest
import java.io.BufferedReader
import java.io.Closeable
import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.DirectoryStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class ReadFilesPK(
        directory: String,
        private val charset: Charset,
        private val glob: String,
        private val parallelism: Int,
        private val id: Int
) : AbstractProcessorK(), Closeable {
    override var isCooperative = true

    private val directory = Paths.get(directory)

    private var directoryStream: DirectoryStream<Path>? = null
    private var currentFileReader: BufferedReader? = null

    @Throws(IOException::class)
    override fun init(context: Processor.Context) {
        directoryStream = Files.newDirectoryStream(directory, glob)
    }

    override suspend fun complete() {
        try {
            directoryStream!!
                .filter({ path ->
                    Files.isRegularFile(path) &&
                            (path.hashCode() and Integer.MAX_VALUE) % parallelism == id })
                .forEach {
                    logFinest(logger, "Processing file ", it)
                    val reader = Files.newBufferedReader(it, charset)
                    currentFileReader = reader
                    reader.useLines { lines ->
                        lines.forEach { emit(it) }
                    }
                }
        } finally {
            close()
            directoryStream = null
        }
    }

    @Throws(IOException::class)
    override fun close() {
        var ex: IOException? = null
        try {
            directoryStream?.close()
        } catch (e: IOException) {
            ex = e
        }
        currentFileReader?.close()
        ex?.apply { throw this }
    }

    companion object {
        @JvmStatic fun metaSupplier(directory:String, charset:String, glob:String): ProcessorMetaSupplier {
            return ProcessorMetaSupplier.of(CloseableProcessorSupplier { count ->
                (0 until count).map { i ->
                    CloseableKotlinWrapperP(ReadFilesPK(directory, Charset.forName(charset), glob, count, i)) }
            }, 2)
        }
    }
}
