/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterMetadata implements IdentifiedDataSerializable {

    private String name;
    private String version;
    private long clusterTime;
    private ClusterState state;
    private Set<Member> members;
    private boolean sslEnabled;

    public ClusterMetadata() {
    }

    public ClusterMetadata(String name, Cluster cluster, boolean sslEnabled) {
        this.name = name;
        this.version = BuildInfoProvider.getBuildInfo().getJetBuildInfo().getVersion();
        this.state = cluster.getClusterState();
        this.clusterTime = cluster.getClusterTime();
        this.members = cluster.getMembers();
        this.sslEnabled = sslEnabled;
    }


    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public ClusterState getState() {
        return state;
    }

    @Nonnull
    public String getVersion() {
        return version;
    }

    @Nonnull
    public long getClusterTime() {
        return clusterTime;
    }

    @Nonnull
    public Set<Member> getMembers() {
        return members;
    }

    @Nonnull
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.CLUSTER_METADATA;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(version);
        out.writeObject(state);
        out.writeInt(members.size());
        for (Member member : members) {
            out.writeObject(member);
        }
        out.writeLong(clusterTime);
        out.writeBoolean(sslEnabled);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        version = in.readUTF();
        state = in.readObject();
        int memberSize = in.readInt();
        if (memberSize > 0) {
            members = new HashSet<>(memberSize);
            for (int i = 0; i < memberSize; i++) {
                members.add(in.readObject());
            }
        }
        clusterTime = in.readLong();
        sslEnabled = in.readBoolean();
    }

    @Override
    public String toString() {
        return "ClusterSummary{" +
                "name='" + name + '\'' +
                ", version='" + version + '\'' +
                ", clusterTime=" + clusterTime +
                ", state=" + state +
                ", sslEnabled=" + sslEnabled +
                ", members=" + members.stream()
                                      .map(Member::getAddress)
                                      .map(Address::toString)
                                      .collect(Collectors.joining(",")) +
                '}';
    }

}
