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

import com.hazelcast.jet.JetException
import com.hazelcast.jet.core.CloseableProcessorSupplier
import com.hazelcast.jet.core.Processor
import com.hazelcast.jet.core.ProcessorMetaSupplier
import com.hazelcast.jet.impl.util.LoggingUtil.logFine
import com.hazelcast.jet.impl.util.LoggingUtil.logFinest
import com.hazelcast.jet.impl.util.ReflectionUtils
import java.io.*
import java.nio.charset.Charset
import java.nio.file.*
import java.nio.file.StandardWatchEventKinds.*
import java.util.*
import java.util.concurrent.TimeUnit.SECONDS

/**
 * The amount of data read from one file at once must be limited
 * in order to prevent a possible [OVERFLOW][java.nio.file.StandardWatchEventKinds.OVERFLOW]
 * if too many Watcher events accumulate in the queue. This
 * constant specifies the number of lines to read at once, before going
 * back to polling the event queue.
 */
private const val LINES_IN_ONE_BATCH = 512
private const val SENSITIVITY_MODIFIER_CLASSNAME = "com.sun.nio.file.SensitivityWatchEventModifier"
private val kindsOfWatchEvents = arrayOf<WatchEvent.Kind<*>>(ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE)

// Modifiers for file watch service to achieve the highest possible
// sensitivity. Background: Java 7 SE defines no standard modifiers for a
// watch service. However some JDKs use internal modifiers to increase
// sensitivity. This field contains modifiers to be used for highest possible
// sensitivity. It's JVM-specific and hence it's just a best-effort.
// I believe this is useful on platforms without native watch service (or where
// Java does not use it) e.g. MacOSX bad luck, we did not find the modifier
private val watchEventModifiers: Array<WatchEvent.Modifier> = run {
    val modifier = ReflectionUtils.readStaticFieldOrNull<Any>(SENSITIVITY_MODIFIER_CLASSNAME, "HIGH")
    if (modifier is WatchEvent.Modifier)
        arrayOf(modifier) else
        arrayOf()
}

/**
 * Private API. Access via [com.hazelcast.jet.core.processor.SourceProcessors.streamFilesP]
 */
class StreamFilesPK(
        watchedDirectory: String,
        private val charset: Charset,
        glob: String,
        private val parallelism: Int,
        private val id: Int
) : AbstractProcessorK(), Closeable {
    override var isCooperative = false

    val fileOffsets: MutableMap<Path, Long> = HashMap()

    private val watchedDirectory = Paths.get(watchedDirectory)
    private val glob = FileSystems.getDefault().getPathMatcher("glob:" + glob)
    private val eventQueue = ArrayDeque<Path>()

    private val lineBuilder = StringBuilder()
    private var watcher: WatchService? = null

    override fun init(context: Processor.Context) {
        Files.newDirectoryStream(watchedDirectory)
                .filter { Files.isRegularFile(it) }
                .forEach {
                    // Negative offset means "initial offset", needed to skip the first line
                    fileOffsets.put(it, -Files.size(it))
                }
        watcher = FileSystems.getDefault().newWatchService()
        watchedDirectory.register(watcher, kindsOfWatchEvents, *watchEventModifiers)
        logger.info("Started to watch directory: " + watchedDirectory)
    }

    override suspend fun complete() {
        this.use { _ ->
            while (receiveWatcherEvents()) {
                eventQueue.poll()?.emitLines()
                yield()
            }
        }
    }

    override fun close() {
        logger.info("Closing StreamFilesPK")
        try {
            closeWatcher()
        } catch (e: IOException) {
            logger.severe("Failed to close the directory watcher", e)
        }
    }

    private fun closeWatcher() {
        try {
            watcher?.close()
        } finally {
            watcher = null
        }
    }

    /**
     * True return value means: "there are pending watcher events in the queue
     * or there may be more in the future (the watcher is still open)."
     *
     * False return value means: "we have closed the directory watcher
     * and processed all pending watcher events."
     */
    private fun receiveWatcherEvents(): Boolean {
        if (watcher == null) {
            return eventQueue.isNotEmpty()
        }
        // poll with blocking only when there is no other work to do
        val key = if (eventQueue.isEmpty())
            watcher!!.poll(1, SECONDS) else
            watcher!!.poll()
        if (key == null) {
            if (!Files.exists(watchedDirectory)) {
                logger.info("Directory $watchedDirectory does not exist. Stopping the directory watcher.")
                closeWatcher()
            }
            return watcher != null || eventQueue.isNotEmpty()
        }
        for (event in key.pollEvents()) {
            val kind = event.kind()
            @Suppress("UNCHECKED_CAST")
            val file = (event as WatchEvent<Path>).context()
            val path = watchedDirectory.resolve(file)
            when (kind) {
                ENTRY_CREATE, ENTRY_MODIFY -> {
                    if (glob.matches(file) && file.belongsToThisProcessor() && path.isRegularFile()) {
                        logFine(logger, "Will open file to read new content: %s", path)
                        eventQueue.add(path)
                    }
                }
                ENTRY_DELETE -> {
                    logFinest(logger, "File was deleted: %s", path)
                    fileOffsets.remove(path)
                }
                OVERFLOW -> {
                    logger.warning("Detected OVERFLOW in " + watchedDirectory)
                }
                else -> {
                    throw JetException("Unknown kind of WatchEvent: " + kind)
                }
            }
            if (!key.reset()) {
                logger.info("Watch key got invalidated. Stopping the directory watcher.")
                closeWatcher()
            }
        }
        return watcher != null || eventQueue.isNotEmpty()
    }

    private fun Path.belongsToThisProcessor(): Boolean {
        return (hashCode() and Integer.MAX_VALUE) % parallelism == id
    }

    private fun Path.isRegularFile() = Files.isRegularFile(this)

    private suspend fun Path.emitLines() {
        val (reader, fis) = this.open() ?: return
        reader.use { _ ->
            while (true) {
                for (i in 1..LINES_IN_ONE_BATCH) {
                    val line = reader.readCompleteLine()
                    if (line == null) {
                        fileOffsets.put(this, fis.channel.position())
                        reader.close()
                        return
                    }
                    emit(line)
                }
                receiveWatcherEvents()
                yield()
            }
        }
    }

    @Throws(IOException::class)
    private fun Path.open(): Pair<Reader, FileInputStream>? {
        val offset = fileOffsets.getOrDefault(this, 0L)
        logFinest(logger, "Processing file %s, previous offset: %,d", this, offset)
        try {
            val fis = FileInputStream(toFile())
            // Negative offset means we're reading the file for the first time.
            // We recover the actual offset by negating, then we subtract one
            // so as not to miss a preceding newline.
            fis.channel.position(if (offset >= 0) offset else -offset - 1)
            val reader = BufferedReader(InputStreamReader(fis, charset))
            if (offset < 0 && !reader.findNextLine()) {
                // we've hit EOF before finding the end of current line
                fileOffsets.put(this, offset)
                reader.close()
                return null
            }
            return Pair(reader, fis)
        } catch (ignored: FileNotFoundException) {
            // This could be caused by ENTRY_MODIFY emitted on file deletion
            // just before ENTRY_DELETE
            return null
        }
    }

    @Throws(IOException::class)
    private fun Reader.findNextLine(): Boolean {
        while (true) {
            val result = this.read()
            if (result < 0) {
                return false
            }
            val ch = result.toChar()
            if (ch == '\n' || ch == '\r') {
                maybeSkipLF(ch)
                return true
            }
        }
    }

    /**
     * Reads a line from the input only if it is terminated by CR or LF or
     * CRLF. If it detects EOF before the newline character, returns `null`.
     *
     * @return The line (possibly zero-length) or null on EOF.
     */
    private fun Reader.readCompleteLine(): String? {
        while (true) {
            val result = this.read()
            if (result < 0) {
                break
            }
            val ch = result.toChar()
            if (ch == '\r' || ch == '\n') {
                maybeSkipLF(ch)
                try {
                    return lineBuilder.toString()
                } finally {
                    lineBuilder.setLength(0)
                }
            } else {
                lineBuilder.append(ch)
            }
        }
        // EOF
        return null
    }

    companion object {

        private fun Reader.maybeSkipLF(ch: Char) {
            // look ahead for possible '\n' after '\r' (windows end-line style)
            if (ch == '\r') {
                mark(1)
                val ch2 = this.read().toChar()
                if (ch2 != '\n') {
                    reset()
                }
            }
        }

        /**
         * Private API. Use [com.hazelcast.jet.core.processor.SourceProcessors.streamFilesP]
         * instead.
         */
        @JvmStatic
        fun metaSupplier(watchedDirectory: String, charset: String, glob: String): ProcessorMetaSupplier {
            return ProcessorMetaSupplier.of(CloseableProcessorSupplier { count ->
                (0 until count)
                        .map { StreamFilesPK(watchedDirectory, Charset.forName(charset), glob, count, it) }
                        .map { CloseableKotlinWrapperP(it) }
                        .toList()
            }, 2)
        }
    }
}
