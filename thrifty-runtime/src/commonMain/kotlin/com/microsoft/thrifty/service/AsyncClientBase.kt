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
package com.microsoft.thrifty.service

import com.microsoft.thrifty.protocol.Protocol
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.IO
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.launch
import okio.Closeable
import kotlin.coroutines.CoroutineContext

/**
 * Implements a basic service client that executes methods asynchronously.
 *
 * Note that, while the client-facing API of this class is callback-based,
 * the implementation itself is **blocking**.  Unlike the Apache
 * implementation, there is no presumption made here about framed encoding
 * at the transport level.  If your backend requires framing, be sure to
 * configure your [Protocol] and [com.microsoft.thrifty.transport.Transport]
 * objects appropriately.
 *
 * @param protocol the [Protocol] used to encode/decode requests and responses.
 * @param dispatcher a [CoroutineDispatcher] on which to run service calls.
 */
@OptIn(ExperimentalCoroutinesApi::class)
open class AsyncClientBase protected constructor(
    protocol: Protocol,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
) : ClientBase(protocol), Closeable {
    private val jobRef = atomic<Job?>(null)
    private val context: CoroutineContext = dispatcher.limitedParallelism(parallelism = 1)
    private val channel = Channel<MethodCallAndChannel>(
        capacity = Channel.UNLIMITED,
        onBufferOverflow = BufferOverflow.SUSPEND,
        onUndeliveredElement = null
    )

    init {
        jobRef.value = CoroutineScope(context + SupervisorJob()).launch {
            dispatchMethodCalls()
        }.also { job ->
            job.invokeOnCompletion { jobRef.compareAndSet(job, null) }
        }
    }

    protected suspend fun enqueue(methodCall: MethodCall<*>): Any? {
        val callAndChannel = MethodCallAndChannel(
            call = methodCall,
            resultChannel = Channel(
                capacity = 1,
                onBufferOverflow = BufferOverflow.DROP_LATEST,
                onUndeliveredElement = null,
            ),
        )

        channel.send(callAndChannel)
        val result = callAndChannel.resultChannel.receive()
        return result.getOrThrow()
    }

    override fun close() {
        channel.close()
        jobRef.getAndSet(null)?.cancel()
        super.close()
    }

    private suspend fun dispatchMethodCalls() {
        while (true) {
            val callAndChannel = try {
                channel.receive()
            } catch (e: ClosedReceiveChannelException) {
                // we're shutting down!
                break
            }

            val result: Result<*> = try {
                Result.success(invokeRequest(callAndChannel.call))
            } catch (e: ServerException) {
                Result.failure<Any>(e.thriftException)
            } catch (t: Throwable) {
                Result.failure<Any>(t)
            }

            try {
                callAndChannel.resultChannel.trySend(result)
            } finally {
                callAndChannel.resultChannel.close()
            }
        }
    }

    private class MethodCallAndChannel(
        val call: MethodCall<*>,
        val resultChannel: Channel<Result<*>>,
    )
}
