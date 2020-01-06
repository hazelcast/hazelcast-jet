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

package com.hazelcast.jet.spring;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.spring.context.SpringAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.Nonnull;

/**
 * Utility class with methods that create several useful Spring Bean
 * {@link ServiceFactory service factories} and transform functions.
 *
 * @since 4.0
 */
public final class JetSpringServices {

    private JetSpringServices() {
    }

    /**
     * Returns a transform function for the {@link BatchStage#apply(FunctionEx)}
     * which adds a mapping stage using the specified bean.
     *
     * @param beanName     the name of the bean
     * @param requiredType the class of the bean
     * @param mapper       the mapping function
     * @param <T>          the type of the bean
     * @param <R>          the type of the returned stage
     */
    public static <I, R, T> FunctionEx<BatchStage<I>, BatchStage<R>> mapBatchUsingSpringBean(
            @Nonnull String beanName,
            @Nonnull Class<T> requiredType,
            @Nonnull BiFunctionEx<? super T, ? super I, ? extends R> mapper
    ) {
        return e -> e.mapUsingService(beanServiceFactory(beanName, requiredType), mapper::apply);
    }

    /**
     * Returns a transform function for the {@link BatchStage#apply(FunctionEx)}
     * which adds a mapping stage using the specified bean.
     *
     * @param requiredType the class of the bean
     * @param mapper       the mapping function
     * @param <T>          the type of the bean
     * @param <R>          the type of the returned stage
     */
    public static <I, R, T> FunctionEx<BatchStage<I>, BatchStage<R>> mapBatchUsingSpringBean(
            @Nonnull Class<T> requiredType,
            @Nonnull BiFunctionEx<? super T, ? super I, ? extends R> mapper
    ) {
        return e -> e.mapUsingService(beanServiceFactory(requiredType), mapper::apply);
    }

    /**
     * Returns a transform function for the {@link BatchStage#apply(FunctionEx)}
     * which adds a mapping stage using the specified bean.
     *
     * @param beanName the name of the bean
     * @param mapper   the mapping function
     * @param <T>      the type of the bean
     * @param <R>      the type of the returned stage
     */
    public static <I, R, T> FunctionEx<BatchStage<I>, BatchStage<R>> mapBatchUsingSpringBean(
            @Nonnull String beanName,
            @Nonnull BiFunctionEx<? super T, ? super I, ? extends R> mapper
    ) {
        return e -> e.mapUsingService(beanServiceFactory(beanName), mapper::apply);
    }

    /**
     * Returns a transform function for the {@link StreamStage#apply(FunctionEx)}
     * which adds a mapping stage using the specified bean.
     *
     * @param beanName     the name of the bean
     * @param requiredType the class of the bean
     * @param mapper       the mapping function
     * @param <T>          the type of the bean
     * @param <R>          the type of the returned stage
     */
    public static <I, R, T> FunctionEx<StreamStage<I>, StreamStage<R>> mapStreamUsingSpringBean(
            @Nonnull String beanName,
            @Nonnull Class<T> requiredType,
            @Nonnull BiFunctionEx<? super T, ? super I, ? extends R> mapper
    ) {
        return e -> e.mapUsingService(beanServiceFactory(beanName, requiredType), mapper::apply);
    }

    /**
     * Returns a transform function for the {@link StreamStage#apply(FunctionEx)}
     * which adds a mapping stage using the specified bean.
     *
     * @param requiredType the class of the bean
     * @param mapper       the mapping function
     * @param <T>          the type of the bean
     * @param <R>          the type of the returned stage
     */
    public static <I, R, T> FunctionEx<StreamStage<I>, StreamStage<R>> mapStreamUsingSpringBean(
            @Nonnull Class<T> requiredType,
            @Nonnull BiFunctionEx<? super T, ? super I, ? extends R> mapper
    ) {
        return e -> e.mapUsingService(beanServiceFactory(requiredType), mapper::apply);
    }

    /**
     * Returns a transform function for the {@link StreamStage#apply(FunctionEx)}
     * which adds a mapping stage using the specified bean.
     *
     * @param beanName the name of the bean
     * @param mapper   the mapping function
     * @param <T>      the type of the bean
     * @param <R>      the type of the returned stage
     */
    public static <I, R, T> FunctionEx<StreamStage<I>, StreamStage<R>> mapStreamUsingSpringBean(
            @Nonnull String beanName,
            @Nonnull BiFunctionEx<? super T, ? super I, ? extends R> mapper
    ) {
        return e -> e.mapUsingService(beanServiceFactory(beanName), mapper::apply);
    }

    /**
     * Returns a transform function for the {@link BatchStage#apply(FunctionEx)}
     * which adds a filter stage using the specified bean.
     *
     * @param beanName     the name of the bean
     * @param requiredType the class of the bean
     * @param predicate    the predicate
     * @param <T>          the type of the bean
     */
    public static <I, T> FunctionEx<BatchStage<I>, BatchStage<I>> filterBatchUsingSpringBean(
            @Nonnull String beanName,
            @Nonnull Class<T> requiredType,
            @Nonnull BiPredicateEx<? super T, ? super I> predicate
    ) {
        return e -> e.filterUsingService(beanServiceFactory(beanName, requiredType), predicate::test);
    }

    /**
     * Returns a transform function for the {@link BatchStage#apply(FunctionEx)}
     * which adds a filter stage using the specified bean.
     *
     * @param requiredType the class of the bean
     * @param predicate    the predicate
     * @param <T>          the type of the bean
     */
    public static <I, T> FunctionEx<BatchStage<I>, BatchStage<I>> filterBatchUsingSpringBean(
            @Nonnull Class<T> requiredType,
            @Nonnull BiPredicateEx<? super T, ? super I> predicate
    ) {
        return e -> e.filterUsingService(beanServiceFactory(requiredType), predicate::test);
    }

    /**
     * Returns a transform function for the {@link BatchStage#apply(FunctionEx)}
     * which adds a filter stage using the specified bean.
     *
     * @param beanName  the name of the bean
     * @param predicate the predicate
     * @param <T>       the type of the bean
     */
    public static <I, T> FunctionEx<BatchStage<I>, BatchStage<I>> filterBatchUsingSpringBean(
            @Nonnull String beanName,
            @Nonnull BiPredicateEx<? super T, ? super I> predicate
    ) {
        return e -> e.filterUsingService(beanServiceFactory(beanName), predicate::test);
    }

    /**
     * Returns a transform function for the {@link StreamStage#apply(FunctionEx)}
     * which adds a filter stage using the specified bean.
     *
     * @param beanName     the name of the bean
     * @param requiredType the class of the bean
     * @param predicate    the predicate
     * @param <T>          the type of the bean
     */
    public static <I, T> FunctionEx<StreamStage<I>, StreamStage<I>> filterStreamUsingSpringBean(
            @Nonnull String beanName,
            @Nonnull Class<T> requiredType,
            @Nonnull BiPredicateEx<? super T, ? super I> predicate
    ) {
        return e -> e.filterUsingService(beanServiceFactory(beanName, requiredType), predicate::test);
    }

    /**
     * Returns a transform function for the {@link StreamStage#apply(FunctionEx)}
     * which adds a filter stage using the specified bean.
     *
     * @param requiredType the class of the bean
     * @param predicate    the predicate
     * @param <T>          the type of the bean
     */
    public static <I, T> FunctionEx<StreamStage<I>, StreamStage<I>> filterStreamUsingSpringBean(
            @Nonnull Class<T> requiredType,
            @Nonnull BiPredicateEx<? super T, ? super I> predicate
    ) {
        return e -> e.filterUsingService(beanServiceFactory(requiredType), predicate::test);
    }

    /**
     * Returns a transform function for the {@link StreamStage#apply(FunctionEx)}
     * which adds a filter stage using the specified bean.
     *
     * @param beanName  the name of the bean
     * @param predicate the predicate
     * @param <T>       the type of the bean
     */
    public static <I, T> FunctionEx<StreamStage<I>, StreamStage<I>> filterStreamUsingSpringBean(
            @Nonnull String beanName,
            @Nonnull BiPredicateEx<? super T, ? super I> predicate
    ) {
        return e -> e.filterUsingService(beanServiceFactory(beanName), predicate::test);
    }

    /**
     * Returns a factory that provides the specified bean as the service object.
     *
     * @param beanName     the name of the bean
     * @param requiredType the class of the bean
     * @param <T>          the type of the bean
     */
    public static <T> ServiceFactory<?, T> beanServiceFactory(@Nonnull String beanName, @Nonnull Class<T> requiredType) {
        return ServiceFactory.withCreateContextFn(ctx -> new BeanExtractor())
                             .withCreateServiceFn((c, g) -> g.getBean(beanName, requiredType));
    }

    /**
     * Returns a factory that provides the specified bean as the service object.
     *
     * @param requiredType the class of the bean
     * @param <T>          the type of the bean
     */
    public static <T> ServiceFactory<?, T> beanServiceFactory(@Nonnull Class<T> requiredType) {
        return ServiceFactory.withCreateContextFn(ctx -> new BeanExtractor())
                             .withCreateServiceFn((c, g) -> g.getBean(requiredType));
    }

    /**
     * Returns a factory that provides the specified bean as the service object.
     *
     * @param beanName the name of the bean
     * @param <T>      the type of the bean
     */
    public static <T> ServiceFactory<?, T> beanServiceFactory(@Nonnull String beanName) {
        return ServiceFactory.withCreateContextFn(ctx -> new BeanExtractor())
                             .withCreateServiceFn((c, g) -> g.getBean(beanName));
    }

    @SpringAware
    private static final class BeanExtractor {

        @Autowired
        private transient ApplicationContext context;

        public <T> T getBean(String name) {
            return (T) context.getBean(name);
        }

        public <T> T getBean(String name, Class<T> type) {
            return context.getBean(name, type);
        }

        public <T> T getBean(Class<T> type) {
            return context.getBean(type);
        }
    }
}
