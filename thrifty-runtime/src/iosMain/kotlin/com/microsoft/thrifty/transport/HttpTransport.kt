/*
 * Thrifty
 *
 * Copyright (c) Microsoft Corporation
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN  *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING
 * WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE,
 * FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing permissions and limitations under the License.
 */
package com.microsoft.thrifty.transport

import kotlinx.atomicfu.atomic
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.usePinned
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.suspendCancellableCoroutine
import okio.IOException
import platform.Foundation.NSMakeRange
import platform.Foundation.NSMutableData
import platform.Foundation.NSMutableURLRequest
import platform.Foundation.NSTimeInterval
import platform.Foundation.NSURL
import platform.Foundation.NSURLSession
import platform.Foundation.appendBytes
import platform.Foundation.dataTaskWithRequest
import platform.Foundation.getBytes
import platform.Foundation.setHTTPBody
import platform.Foundation.setHTTPMethod
import platform.Foundation.setValue
import kotlin.coroutines.cancellation.CancellationException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

@OptIn(ExperimentalForeignApi::class)
actual class HttpTransport actual constructor(url: String) : Transport {
    private val url = NSURL.URLWithString(url)!!
    private val customHeaders = mutableMapOf<String, String>()

    private var readTimeout: NSTimeInterval = 0.0
    private var connectTimeout: NSTimeInterval = 0.0

    private var writing: Boolean = true

    // When writing, [data] will act as a send buffer, sent on [flush].
    // When reading, will hold response bytes that can be read out
    // by calls to [read], and [consumed] will track how many bytes have
    // been read out.
    private var data: NSMutableData = NSMutableData()
    private var consumed = 0UL

    private val pendingContinuation = atomic<CancellableContinuation<Unit>?>(null)

    override fun close() {
        pendingContinuation.getAndSet(null)?.cancel()
    }

    @Throws(CancellationException::class, IllegalArgumentException::class, IOException::class)
    override suspend fun read(buffer: ByteArray, offset: Int, count: Int): Int {
        require(!writing) { "Cannot read before calling flush()" }
        require(count > 0) { "Cannot read a negative or zero number of bytes" }
        require(offset >= 0) { "Cannot read into a negative offset" }
        require(offset < buffer.size) { "Offset is outside of buffer bounds" }
        require(offset + count <= buffer.size) { "Not enough room in buffer for requested read" }


        val remaining = data.length() - consumed
        val toCopy = minOf(remaining, count.convert())

        buffer.usePinned { pinned ->
            data.getBytes(pinned.addressOf(offset), NSMakeRange(consumed.convert(), toCopy.convert()))
        }

        // If we copied bytes, move the pointer.
        if (toCopy > 0U) {
            consumed += toCopy
        }

        return toCopy.convert()
    }

    @Throws(CancellationException::class, IllegalArgumentException::class, IOException::class)
    override suspend fun write(buffer: ByteArray, offset: Int, count: Int) {
        require(offset >= 0) { "offset < 0: $offset" }
        require(count >= 0) { "count < 0: $count" }
        require(offset + count <= buffer.size) { "offset + count > buffer.size: $offset + $count > ${buffer.size}" }

        if (!writing) {
            // Maybe there's still data in the buffer to be read,
            // but if our user is writing, then let's just go with it.
            pendingContinuation.getAndSet(null)?.cancel()

            data.setLength(0U)
            consumed = 0U
            writing = true
        }

        buffer.usePinned { pinned ->
            data.appendBytes(pinned.addressOf(offset), count.convert())
        }
    }

    @Throws(CancellationException::class, IllegalStateException::class, IOException::class)
    override suspend fun flush() {
        check(writing) { "Cannot flush after calling read()" }

        val urlRequest = NSMutableURLRequest(url)
        urlRequest.setHTTPMethod("POST")
        urlRequest.setValue(value = "application/x-thrift", forHTTPHeaderField = "Content-Type")
        urlRequest.setValue(value = "application/x-thrift", forHTTPHeaderField = "Accept")
        urlRequest.setValue(value = "Java/THttpClient", forHTTPHeaderField = "User-Agent")

        for ((key, value) in customHeaders) {
            urlRequest.setValue(value, forHTTPHeaderField = key)
        }

        if (readTimeout != 0.0) {
            urlRequest.setTimeoutInterval(readTimeout)
        }

        urlRequest.setHTTPBody(data)

        val session = NSURLSession.sharedSession()

        suspendCancellableCoroutine { cont ->
            val task = session.dataTaskWithRequest(urlRequest) { data, _, err ->
                if (data != null) {
                    this.data = data.mutableCopy() as NSMutableData
                } else {
                    this.data.setLength(0U)
                }

                this.consumed = 0U
                this.writing = false

                if (err != null) {
                    cont.resumeWithException(IOException("Request failed: $err"))
                } else {
                    cont.resume(Unit)
                }
            }

            cont.invokeOnCancellation { task.cancel() }

            task.resume()
        }
    }

    actual fun setConnectTimeout(timeout: Int) {
        this.connectTimeout = millisToTimeInterval(timeout.toLong())
    }

    actual fun setReadTimeout(timeout: Int) {
        this.readTimeout = millisToTimeInterval(timeout.toLong())
    }

    actual fun setCustomHeaders(headers: Map<String, String>) {
        customHeaders.clear()
        customHeaders.putAll(headers)
    }

    actual fun setCustomHeader(key: String, value: String) {
        customHeaders[key] = value
    }
}

fun millisToTimeInterval(millis: Long): NSTimeInterval {
    // NSTimeInterval is a double-precision floating point number representing
    // seconds.  So to go from millis to NSTimeInterval, we divide by 1000.0.
    return millis / 1000.0
}
