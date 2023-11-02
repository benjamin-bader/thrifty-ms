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
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
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
 * @param listener a callback object to receive client-level events.
 */
@OptIn(ExperimentalCoroutinesApi::class)
open class AsyncClientBase protected constructor(
    protocol: Protocol,
    dispatcher: CoroutineDispatcher = Dispatchers.Default,
) : ClientBase(protocol), Closeable {
    private val context: CoroutineContext = dispatcher.limitedParallelism(parallelism = 1)
    private val jobRef = atomic<Job?>(SupervisorJob())
    private val scope: CoroutineScope = CoroutineScope(context + jobRef.value!!)

    init {
        jobRef.value!!.let { job ->
            job.invokeOnCompletion { jobRef.compareAndSet(job, null) }
        }
    }

    protected suspend fun enqueue(methodCall: MethodCall<*>): Any? {
        return scope.async {
            try {
                invokeRequest(methodCall)
            } catch (e: ServerException) {
                throw e.thriftException
            }
        }.await()
    }

    override fun close() {
        jobRef.getAndSet(null)?.cancel()
        super.close()
    }
}
