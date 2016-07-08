/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.container;

import com.hazelcast.jet.application.ApplicationListener;
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.jet.container.ContainerListener;
import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.jet.executor.TaskContext;
import com.hazelcast.jet.io.DataType;
import com.hazelcast.jet.io.ObjectReaderFactory;
import com.hazelcast.jet.io.ObjectWriterFactory;
import com.hazelcast.spi.NodeEngine;

import java.io.Serializable;

public class DefaultProcessorContext implements ProcessorContext {
    private final TaskContext taskContext;
    private final ContainerContext containerContext;

    public DefaultProcessorContext(TaskContext taskContext,
                                   ContainerContext containerContext) {
        this.taskContext = taskContext;
        this.containerContext = containerContext;
    }

    @Override
    public NodeEngine getNodeEngine() {
        return this.containerContext.getNodeEngine();
    }

    @Override
    public String getApplicationName() {
        return this.containerContext.getApplicationName();
    }

    @Override
    public int getID() {
        return this.containerContext.getID();
    }

    @Override
    public Vertex getVertex() {
        return this.containerContext.getVertex();
    }

    @Override
    public DAG getDAG() {
        return this.containerContext.getDAG();
    }

    @Override
    public JetTupleFactory getTupleFactory() {
        return this.containerContext.getTupleFactory();
    }

    @Override
    public ApplicationConfig getConfig() {
        return this.containerContext.getConfig();
    }

    @Override
    public void registerContainerListener(String vertexName, ContainerListener containerListener) {
        this.containerContext.registerContainerListener(vertexName, containerListener);
    }

    @Override
    public void registerApplicationListener(ApplicationListener applicationListener) {
        this.containerContext.registerApplicationListener(applicationListener);
    }

    @Override
    public <T> void putApplicationVariable(String variableName, T variable) {
        this.containerContext.putApplicationVariable(variableName, variable);
    }

    @Override
    public <T> T getApplicationVariable(String variableName) {
        return this.containerContext.getApplicationVariable(variableName);
    }

    @Override
    public void cleanApplicationVariable(String variableName) {
        this.containerContext.cleanApplicationVariable(variableName);
    }

    @Override
    public void registerDataType(DataType dataType) {
        this.taskContext.registerDataType(dataType);
    }

    @Override
    public ObjectReaderFactory getObjectReaderFactory() {
        return this.taskContext.getObjectReaderFactory();
    }

    @Override
    public ObjectWriterFactory getObjectWriterFactory() {
        return this.taskContext.getObjectWriterFactory();
    }

    @Override
    public int getTaskCount() {
        return this.taskContext.getTaskCount();
    }

    @Override
    public int getTaskNumber() {
        return this.taskContext.getTaskNumber();
    }

    @Override
    public <V, R extends Serializable> Accumulator<V, R> getAccumulator(String key) {
        return this.taskContext.getAccumulator(key);
    }

    @Override
    public <V, R extends Serializable> void setAccumulator(String key, Accumulator<V, R> accumulator) {
        this.taskContext.setAccumulator(key, accumulator);
    }
}
