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

package com.hazelcast.jet.impl.client.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec;
import com.hazelcast.client.impl.protocol.codec.builtin.StringCodec;
import com.hazelcast.jet.impl.ClusterMetadata;
import com.hazelcast.nio.Bits;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public final class ClusterMetadataCodec {
    private static final int CLUSTER_TIME_OFFSET = 0;
    private static final int STATE_OFFSET = CLUSTER_TIME_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = STATE_OFFSET + Bits.INT_SIZE_IN_BYTES;

    private ClusterMetadataCodec() {
    }

    public static void encode(ClientMessage clientMessage, ClusterMetadata metadata) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        FixedSizeTypesCodec.encodeLong(initialFrame.content, CLUSTER_TIME_OFFSET, metadata.getClusterTime());
        FixedSizeTypesCodec.encodeInt(initialFrame.content, STATE_OFFSET, metadata.getStateOrdinal());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, metadata.getName());
        StringCodec.encode(clientMessage, metadata.getVersion());

        clientMessage.add(END_FRAME);
    }

    public static ClusterMetadata decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        long clusterTime = FixedSizeTypesCodec.decodeLong(initialFrame.content, CLUSTER_TIME_OFFSET);
        int state = FixedSizeTypesCodec.decodeInt(initialFrame.content, STATE_OFFSET);

        String name = StringCodec.decode(iterator);
        String version = StringCodec.decode(iterator);
        fastForwardToEndFrame(iterator);

        ClusterMetadata clusterMetadata = new ClusterMetadata();
        clusterMetadata.setClusterTime(clusterTime);
        clusterMetadata.setState(state);
        clusterMetadata.setName(name);
        clusterMetadata.setVersion(version);
        return clusterMetadata;

    }
}
