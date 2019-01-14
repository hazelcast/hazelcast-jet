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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.MasterContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.readList;
import static com.hazelcast.jet.impl.util.Util.writeList;

public class VertexDef implements IdentifiedDataSerializable {

    private int id;
    private List<EdgeDef> inboundEdges = new ArrayList<>();
    private List<EdgeDef> outboundEdges = new ArrayList<>();
    private String name;
    private ProcessorSupplier processorSupplier;
    private int localParallelism;

    VertexDef() {
    }

    VertexDef(int id, String name, ProcessorSupplier processorSupplier, int localParallelism) {
        this.id = id;
        this.name = name;
        this.processorSupplier = processorSupplier;
        this.localParallelism = localParallelism;
    }

    String name() {
        return name;
    }

    int localParallelism() {
        return localParallelism;
    }

    int vertexId() {
        return id;
    }

    void addInboundEdges(List<EdgeDef> edges) {
        this.inboundEdges.addAll(edges);
    }

    void addOutboundEdges(List<EdgeDef> edges) {
        this.outboundEdges.addAll(edges);
    }

    List<EdgeDef> inboundEdges() {
        return inboundEdges;
    }

    List<EdgeDef> outboundEdges() {
        return outboundEdges;
    }

    ProcessorSupplier processorSupplier() {
        return processorSupplier;
    }

    boolean isSnapshotVertex() {
        return name.startsWith(MasterContext.SNAPSHOT_VERTEX_PREFIX);
    }

    /**
     * Returns true in any of the following cases:<ul>
     *     <li>this vertex is a higher-priority source for some of its
     *         downstream vertices
     *     <li>this vertex' output is connected by a snapshot restore edge*
     *     <li>it sits upstream of a vertex meeting the other conditions
     * </ul>
     *
     * (*) We consider a snapshot-restoring vertices to by higher priority
     * because when connected to a source vertex, they are the only input to it
     * and therefore they wouldn't be a higher-priority source. However, we
     * want to prevent snapshot before snapshot restoring is done.
     * Fixes https://github.com/hazelcast/hazelcast-jet/pull/1101
     */
    boolean isHigherPriorityUpstream() {
        for (EdgeDef outboundEdge : outboundEdges) {
            VertexDef downstream = outboundEdge.destVertex();
            if (downstream.inboundEdges.stream()
                                       .anyMatch(edge -> edge.priority() > outboundEdge.priority())
                    || outboundEdge.isSnapshotRestoreEdge()
                    || downstream.isHigherPriorityUpstream()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "VertexDef{" +
                "name='" + name + '\'' +
                '}';
    }

    //             IdentifiedDataSerializable implementation

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.VERTEX_DEF;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        writeList(out, inboundEdges);
        writeList(out, outboundEdges);
        CustomClassLoadedObject.write(out, processorSupplier);
        out.writeInt(localParallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readInt();
        name = in.readUTF();
        inboundEdges = readList(in);
        outboundEdges = readList(in);
        processorSupplier = CustomClassLoadedObject.read(in);
        localParallelism = in.readInt();
    }
}
