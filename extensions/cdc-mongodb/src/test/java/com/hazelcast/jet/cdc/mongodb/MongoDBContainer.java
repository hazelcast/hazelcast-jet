/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;

import java.util.Collections;
import java.util.Set;

public class MongoDBContainer extends GenericContainer<MongoDBContainer> {

    public static final String VERSION = "4.1.13";
    public static final Integer MONGODB_PORT = 27017;

    private static final String IMAGE_NAME = "mongo";
    private static final String WITH_RS_COMMAND = "mongod --bind_ip_all --replSet ";

    private String replicaSetName;

    public MongoDBContainer() {
        this(IMAGE_NAME + ":" + VERSION);
    }

    public MongoDBContainer(String imageName) {
        super(imageName);
    }

    @Override
    protected void configure() {
        addExposedPort(MONGODB_PORT);
        if (replicaSetName != null) {
            withCommand(WITH_RS_COMMAND + replicaSetName);
        }
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return Collections.singleton(getMappedPort(MONGODB_PORT));
    }

    /**
     * Set replicaSet name. If set, replicaSet should be initialized via {@link
     * #initializeReplicaSet()}.
     *
     * @param replicaSetName Enables replicaSet.
     */
    public MongoDBContainer withReplicaSetName(String replicaSetName) {
        this.replicaSetName = replicaSetName;
        return self();
    }

    /**
     * Initialize the replicaSet by calling `rs.initialize()` in mongo shell.
     */
    public int initializeReplicaSet() {
        ExecResult execResult;
        try {
            execResult = execInContainer("mongo", "--eval", "rs.initiate()");
            return execResult.getExitCode();
        } catch (Exception e) {
            throw new ContainerLaunchException("Error during initialization of replicaSet for container: " + self(), e);
        }
    }

    /**
     * @return the connection string to MongoDB
     */
    public String connectionString() {
        return "mongodb://" + getContainerIpAddress() + ":" + getMappedPort(MONGODB_PORT);
    }

    /**
     * @return a new MongoDB client
     */
    public MongoClient newMongoClient() {
        return MongoClients.create(connectionString());
    }
}
