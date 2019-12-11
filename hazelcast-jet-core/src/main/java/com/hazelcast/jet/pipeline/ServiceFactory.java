/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ProcessorSupplier.Context;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * A holder of functions needed to create and destroy a service object.
 * If you don't need fine grained control over the lifecycle of the
 * service, it is recommended to use the simpler
 * {@link ServiceFactories#processorLocalService(SupplierEx, ConsumerEx) ServiceFactories.processorLocalService}
 * or {@link ServiceFactories#sharedService(SupplierEx, ConsumerEx) ServiceFactories.memberLocalService}.
 * <p>
 * The lifecycle of this factory object is as follows:
 * <ol>
 *     <li>The {@code ServiceFactory} is serialized and distributed to all the nodes</li>
 *     <li>
 *         {@link #createContextFn()} is called once per node. A single context
 *         instance exits per transform and node. Any node-wide initialization
 *         should be performed in this step. The context instance will be used
 *         further on to create the actual service instances. For example, the context
 *         instance can create a client for an external service and this client can be
 *         used to create further sessions.
 *     </li>
 *     <li>
 *         Depending on the {@link GeneralStage#setLocalParallelism(int) localParallelism}
 *         of the transform, l{@link #createServiceFn()}} is called once per
 *         processor taking the previously created context instance as input.
 *      </li>
 *      <li>
 *          After the job completes, {@link #destroyServiceFn()} is called
 *          on each service.
 *      </li>
 *      <li>Finally, {@link #destroyContextFn()} is called for instance-wide cleanup.</li>
 * </ol>
 * You can use the service factory from these Pipeline API methods:
 * <ul>
 *     <li>{@link GeneralStage#mapUsingService}
 *     <li>{@link GeneralStage#filterUsingService}
 *     <li>{@link GeneralStage#flatMapUsingService}
 *     <li>{@link GeneralStage#mapUsingServiceAsync}
 *     <li>{@link GeneralStage#filterUsingServiceAsync}
 *     <li>{@link GeneralStage#flatMapUsingServiceAsync}
 *     <li>{@link GeneralStageWithKey#mapUsingService}
 *     <li>{@link GeneralStageWithKey#filterUsingService}
 *     <li>{@link GeneralStageWithKey#flatMapUsingService}
 *     <li>{@link GeneralStageWithKey#mapUsingServiceAsync}
 *     <li>{@link GeneralStageWithKey#filterUsingServiceAsync}
 *     <li>{@link GeneralStageWithKey#flatMapUsingServiceAsync}
 * </ul>
 *
 * @param <C> The service-context object,
 *            which is used to create the per-processor services
 * @param <S> The service instance used in mapping
 *
 * @since 4.0
 */
public final class ServiceFactory<C, S> implements Serializable {

    /**
     * Default value for {@link #maxPendingCallsPerProcessor}.
     */
    public static final int MAX_PENDING_CALLS_DEFAULT = 256;

    /**
     * Default value for {@link #isCooperative}.
     */
    public static final boolean COOPERATIVE_DEFAULT = true;

    /**
     * Default value for {@link #hasOrderedAsyncResponses}.
     */
    public static final boolean ORDERED_ASYNC_RESPONSES_DEFAULT = true;

    private final boolean isCooperative;

    // options for async
    private final int maxPendingCallsPerProcessor;
    private final boolean orderedAsyncResponses;

    @Nonnull
    private final FunctionEx<? super Context, ? extends C> createContextFn;
    @Nonnull
    private final BiFunctionEx<? super Processor.Context, ? super C, ? extends S> createServiceFn;
    @Nonnull
    private final ConsumerEx<? super S> destroyServiceFn;
    @Nonnull
    private final ConsumerEx<? super C> destroyContextFn;

    private ServiceFactory(
            @Nonnull FunctionEx<? super ProcessorSupplier.Context, ? extends C> createContextFn,
            @Nonnull BiFunctionEx<? super Processor.Context, ? super C, ? extends S> createServiceFn,
            @Nonnull ConsumerEx<? super S> destroyServiceFn,
            @Nonnull ConsumerEx<? super C> destroyContextFn,
            boolean isCooperative,
            int maxPendingCallsPerProcessor,
            boolean orderedAsyncResponses
    ) {
        this.createContextFn = createContextFn;
        this.createServiceFn = createServiceFn;
        this.destroyServiceFn = destroyServiceFn;
        this.destroyContextFn = destroyContextFn;
        this.isCooperative = isCooperative;
        this.maxPendingCallsPerProcessor = maxPendingCallsPerProcessor;
        this.orderedAsyncResponses = orderedAsyncResponses;
    }

    /**
     * Creates a new {@link ServiceFactory} with the given function.
     * Make sure to also call {@link #withCreateServiceFn}, otherwise you will
     * not be able to create the service instance. If you want to use
     * the context object as the service, use {@link
     * ServiceFactories#sharedService}.
     *
     * @param createContextFn the function to create new context object, given
     *                        a {@link ProcessorSupplier.Context}. This function is
     *                        called once per node.
     * @param <C> type of the service context instance
     *
     * @return a new factory instance
     */
    @Nonnull
    public static <C> ServiceFactory<C, Void> withCreateContextFn(
            @Nonnull FunctionEx<? super ProcessorSupplier.Context, ? extends C> createContextFn
    ) {
        checkSerializable(createContextFn, "createContextFn");
        return new ServiceFactory<>(
                createContextFn,
                (ctx, svcContext) -> {
                    throw new IllegalStateException("No createServiceFn was given in ServiceFactory");
                },
                ConsumerEx.noop(),
                ConsumerEx.noop(),
                COOPERATIVE_DEFAULT, MAX_PENDING_CALLS_DEFAULT, ORDERED_ASYNC_RESPONSES_DEFAULT
        );
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the
     * context-destroy-function replaced with the given function.
     * <p>
     * The destroy function is called at the end of the job to destroy all
     * created context objects.
     *
     * @param destroyContextFn the function to destroy the service context.
     *                         This function is called once per node
     * @return a copy of this factory with the supplied destroy-function
     */
    @Nonnull
    public ServiceFactory<C, S> withDestroyContextFn(@Nonnull ConsumerEx<? super C> destroyContextFn) {
        checkSerializable(destroyContextFn, "destroyContextFn");
        return new ServiceFactory<>(createContextFn, createServiceFn, destroyServiceFn, destroyContextFn,
                isCooperative, maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the
     * given create-service function.
     * <p>
     * The create-service function is called once per processor to initialize
     * the service from the previously created context.
     * <p>
     * If you update this method, make sure to update
     * {@link #withDestroyServiceFn(ConsumerEx)} as well, since this
     * method resets it.
     *
     * @param createServiceFn the function to create the service instance with the
     *                        local {@link Processor.Context} and the previously
     *                        created context object. This function is called once
     *                        per processor
     * @return a copy of this factory with the supplied create-service-function
     */
    @Nonnull
    public <S_NEW> ServiceFactory<C, S_NEW> withCreateServiceFn(
            @Nonnull BiFunctionEx<? super Processor.Context, ? super C, ? extends S_NEW> createServiceFn
    ) {
        checkSerializable(createServiceFn, "initFn");
        return new ServiceFactory<>(createContextFn, createServiceFn, ConsumerEx.noop(), destroyContextFn,
                isCooperative, maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the
     * service-destroy-function replaced with the given function.
     * <p>
     * The destroy function is called at the end of the job to destroy all
     * created services objects.
     *
     * @param destroyServiceFn the function to destroy the service instance.
     *                         This function is called once per processor instance
     * @return a copy of this factory with the supplied destroy-function
     */
    @Nonnull
    public ServiceFactory<C, S> withDestroyServiceFn(@Nonnull ConsumerEx<? super S> destroyServiceFn) {
        checkSerializable(destroyServiceFn, "destroyServiceFn");
        return new ServiceFactory<>(createContextFn, createServiceFn, destroyServiceFn, destroyContextFn,
                isCooperative, maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the
     * <em>isCooperative</em> flag set to {@code false}. The service factory is
     * cooperative by default. Call this method if your transform function
     * doesn't follow the {@linkplain Processor#isCooperative() cooperative
     * processor contract}, that is if it waits for IO, blocks for
     * synchronization, takes too long to complete etc. If you intend to use
     * the factory for an async operation, you also typically can use a
     * cooperative processor. Cooperative processors offer higher performance.
     *
     * @return a copy of this factory with the {@code isCooperative} flag set
     * to {@code false}.
     */
    @Nonnull
    public ServiceFactory<C, S> toNonCooperative() {
        return new ServiceFactory<>(
                createContextFn, createServiceFn, destroyServiceFn, destroyContextFn,
                false, maxPendingCallsPerProcessor, orderedAsyncResponses
        );
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the
     * <em>maxPendingCallsPerProcessor</em> property set to the given value. Jet
     * will execute at most this many concurrent async operations per processor
     * and will apply backpressure to the upstream.
     * <p>
     * If you use the same service factory on multiple pipeline stages, each
     * stage will count the pending calls independently.
     * <p>
     * This value is ignored when the {@code ServiceFactory} is used in a
     * synchronous transformation.
     * <p>
     * Default value is {@value #MAX_PENDING_CALLS_DEFAULT}.
     *
     * @return a copy of this factory with the {@code maxPendingCallsPerProcessor}
     *      property set.
     */
    @Nonnull
    public ServiceFactory<C, S> withMaxPendingCallsPerProcessor(int maxPendingCallsPerProcessor) {
        checkPositive(maxPendingCallsPerProcessor, "maxPendingCallsPerProcessor must be >= 1");
        return new ServiceFactory<>(
                createContextFn, createServiceFn, destroyServiceFn, destroyContextFn,
                isCooperative, maxPendingCallsPerProcessor, orderedAsyncResponses
        );
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the
     * <em>unorderedAsyncResponses</em> flag set to true.
     * <p>
     * Jet can process asynchronous responses in two modes:
     * <ol><li>
     *     <b>Unordered:</b> results of the async calls are emitted as they
     *     arrive. This mode is enabled by this method.
     * </li><li>
     *     <b>Ordered:</b> results of the async calls are emitted in the submission
     *     order. This is the default.
     * </ol>
     * The unordered mode can be faster:
     * <ul><li>
     *     in the ordered mode, one stalling call will block all subsequent items,
     *     even though responses for them were already received
     * </li><li>
     *     to preserve the order after a restart, the ordered implementation when
     *     saving the state to the snapshot waits for all async calls to complete.
     *     This creates a hiccup depending on the async call latency. The unordered
     *     one saves in-flight items to the state snapshot.
     * </ul>
     * The order of watermarks is preserved even in the unordered mode. Jet
     * forwards the watermark after having emitted all the results of the items
     * that came before it. One stalling response will prevent a windowed
     * operation downstream from finishing, but if the operation is configured
     * to emit early results, they will be more correct with the unordered
     * approach.
     * <p>
     * This value is ignored when the {@code ServiceFactory} is used in a
     * synchronous transformation: the output is always ordered in this case.
     *
     * @return a copy of this factory with the {@code unorderedAsyncResponses} flag set.
     */
    @Nonnull
    public ServiceFactory<C, S> withUnorderedAsyncResponses() {
        return new ServiceFactory<>(
                createContextFn, createServiceFn, destroyServiceFn, destroyContextFn,
                isCooperative, maxPendingCallsPerProcessor, false
        );
    }

    /**
     * Returns the instance-wide create context function.
     *
     * @see #withCreateContextFn(FunctionEx)
     */
    @Nonnull
    public FunctionEx<? super ProcessorSupplier.Context, ? extends C> createContextFn() {
        return createContextFn;
    }

    /**
     * Returns the processor-local service creation function.
     *
     * @see #withCreateServiceFn(BiFunctionEx)
     */
    @Nonnull
    public BiFunctionEx<? super Processor.Context, ? super C, ? extends S> createServiceFn() {
        return createServiceFn;
    }

    /**
     * Returns the processor-local destroy service function.
     *
     * @see #withDestroyServiceFn(ConsumerEx)
     */
    @Nonnull
    public ConsumerEx<? super S> destroyServiceFn() {
        return destroyServiceFn;
    }

    /**
     * Returns the instance-wide destroy context wide.
     *
     * @see #withDestroyContextFn(ConsumerEx)
     */
    @Nonnull
    public ConsumerEx<? super C> destroyContextFn() {
        return destroyContextFn;
    }

    /**
     * Returns the {@code isCooperative} flag.
     */
    public boolean isCooperative() {
        return isCooperative;
    }

    /**
     * Returns the maximum pending calls per processor, see {@link
     * #withMaxPendingCallsPerProcessor(int)}.
     */
    public int maxPendingCallsPerProcessor() {
        return maxPendingCallsPerProcessor;
    }

    /**
     * Tells whether the async responses are ordered, see {@link
     * #withUnorderedAsyncResponses()}.
     */
    public boolean hasOrderedAsyncResponses() {
        return orderedAsyncResponses;
    }
}
