/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.SplitBrainTestSupportTest;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * A support class for high-level split-brain tests. It will form a
 * cluster, create a split-brain situation, and then heal the cluster
 * again.
 * <p>
 * Tests are supposed to extend this class and use its hooks to be
 * notified about state transitions. See
 * {@link #onBeforeSplitBrainCreated(JetInstance[])},
 * {@link #onAfterSplitBrainCreated(JetInstance[], JetInstance[])},
 * and {@link #onAfterSplitBrainHealed(JetInstance[])}
 * <p>
 * The current implementation always isolate the 1st member of the cluster,
 * but it should be simple to customize this class to support mode advanced
 * split-brain scenarios.
 * <p>
 * See {@link SplitBrainTestSupportTest} for an example test.
 */
public abstract class JetSplitBrainTestSupport extends JetTestSupport {

    static final int PARALLELISM = 4;
    static final int NODE_COUNT = 4;
    private static final int[] DEFAULT_BRAINS = new int[]{1, 2};
    private static final int DEFAULT_ITERATION_COUNT = 1;

    private static final SplitBrainAction BLOCK_COMMUNICATION = SplitBrainTestSupport::blockCommunicationBetween;
    private static final SplitBrainAction UNBLOCK_COMMUNICATION = SplitBrainTestSupport::unblockCommunicationBetween;
    private static final SplitBrainAction CLOSE_CONNECTION = HazelcastTestSupport::closeConnectionBetween;
    private static final SplitBrainAction UNBLACKLIST_MEMBERS = JetSplitBrainTestSupport::unblacklistJoinerBetween;


    private JetInstance[] instances;
    private int[] brains;

    /**
     * If new nodes have been created during split brain via
     * {@link #createHazelcastInstanceInBrain(int)}, then their joiners
     * are initialized with the other brain's addresses being blacklisted.
     */
    private boolean unblacklistHint;

    @Before
    public final void setUpInternals() {
        onBeforeSetup();
        final JetConfig config = config();

        brains = brains();
        validateBrainsConfig(brains);
        int clusterSize = getClusterSize();
        instances = startInitialCluster(config, clusterSize);
    }

    @Test
    public void testSplitBrain() throws Exception {
        for (int i = 0; i < iterations(); i++) {
            doIteration();
        }
    }

    /**
     * Override this for custom Hazelcast configuration
     */
    protected JetConfig config() {
        final JetConfig jetConfig = new JetConfig();
        jetConfig.getInstanceConfig().setCooperativeThreadCount(PARALLELISM);
        jetConfig.getHazelcastConfig().setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        jetConfig.getHazelcastConfig().setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
        return jetConfig;
    }

    /**
     * Override this to create a custom brain sizes
     */
    protected int[] brains() {
        return DEFAULT_BRAINS;
    }


    /**
     * Override this to create the split-brain situation multiple-times. The test will use
     * the same members for all iterations.
     */
    protected int iterations() {
        return DEFAULT_ITERATION_COUNT;
    }

    /**
     * Override this method to execute initialization that may be required
     * before instantiating the cluster. This is the
     * first method executed by {@code @Before SplitBrainTestSupport.setupInternals}.
     */
    protected void onBeforeSetup() {

    }

    /**
     * Called when a cluster is fully formed. You can use this method for
     * test initialization, data load, etc.
     *
     * @param instances all Hazelcast instances in your cluster
     */
    protected void onBeforeSplitBrainCreated(JetInstance[] instances) throws Exception {

    }

    /**
     * Called just after a split brain situation was created
     */
    protected void onAfterSplitBrainCreated(JetInstance[] firstBrain, JetInstance[] secondBrain) throws Exception {

    }

    /**
     * Called just after the original cluster was healed again.
     * This is likely the place for various asserts.
     *
     * @param instances all Hazelcast instances in your cluster
     */
    protected void onAfterSplitBrainHealed(JetInstance[] instances) throws Exception {

    }

    /**
     * Indicates whether test should fail when cluster does not include
     * all original members after communications are unblocked.
     * <p>
     * Override this method when it is expected that after communications are
     * unblocked some members will not rejoin the cluster. When overriding this
     * method, it may be desirable to add some wait time to allow the split
     * brain handler to execute.
     *
     * @return {@code true} if the test should fail when not all original members rejoin
     *         after split brain is
     * healed, otherwise {@code false}.
     */
    protected boolean shouldAssertAllNodesRejoined() {
        return true;
    }

    protected JetInstance[] startInitialCluster(JetConfig config, int clusterSize) {
        JetInstance[] jetInstances = new JetInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            JetInstance hz = createJetMember(config);
            jetInstances[i] = hz;
        }
        return jetInstances;
    }

    private void doIteration() throws Exception {
        onBeforeSplitBrainCreated(instances);

        createSplitBrain();
        Brains brains = getBrains();
        onAfterSplitBrainCreated(brains.getFirstHalf(), brains.getSecondHalf());

        healSplitBrain();
        onAfterSplitBrainHealed(instances);
    }


    /**
     * Starts a new {@code JetInstance} which is only able to communicate
     * with members on one of the two brains.
     *
     * @param brain index of brain to start a new instance in (0 to start instance in first
     *              brain or 1 to start instance in second brain)
     * @return a HazelcastInstance whose {@code MockJoiner} has blacklisted the other brain's
     *         members and its connection manager blocks connections to other brain's members
     * @see TestHazelcastInstanceFactory#newHazelcastInstance(Address, com.hazelcast.config.Config, Address[])
     */
    protected JetInstance createHazelcastInstanceInBrain(int brain) {
        Address newMemberAddress = nextAddress();
        Brains brains = getBrains();
        JetInstance[] instancesToBlock = brain == 1 ? brains.firstHalf : brains.secondHalf;

        List<Address> addressesToBlock = new ArrayList<Address>(instancesToBlock.length);
        for (int i = 0; i < instancesToBlock.length; i++) {
            if (isInstanceActive(instancesToBlock[i])) {
                addressesToBlock.add(getAddress(instancesToBlock[i].getHazelcastInstance()));
                // block communication from these instances to the new address

                FirewallingConnectionManager connectionManager = getFireWalledConnectionManager(
                        instancesToBlock[i].getHazelcastInstance());
                connectionManager.blockNewConnection(newMemberAddress);
                connectionManager.closeActiveConnection(newMemberAddress);
            }
        }
        // indicate we need to unblacklist addresses from joiner when split-brain will be healed
        unblacklistHint = true;
        // create a new Hazelcast instance which has blocked addresses blacklisted in its joiner
        return createJetMember(config(), addressesToBlock.toArray(new Address[addressesToBlock.size()]));
    }

    private void validateBrainsConfig(int[] clusterTopology) {
        if (clusterTopology.length != 2) {
            throw new AssertionError("Only simple topologies with 2 brains are supported. Current setup: "
                    + Arrays.toString(clusterTopology));
        }
    }

    private int getClusterSize() {
        int clusterSize = 0;
        for (int brainSize : brains) {
            clusterSize += brainSize;
        }
        return clusterSize;
    }

    private void createSplitBrain() {
        blockCommunications();
        closeExistingConnections();
        assertSplitBrainCreated();
    }

    private void assertSplitBrainCreated() {
        int firstHalfSize = brains[0];
        for (int isolatedIndex = 0; isolatedIndex < firstHalfSize; isolatedIndex++) {
            JetInstance isolatedInstance = instances[isolatedIndex];
            assertClusterSizeEventually(firstHalfSize, isolatedInstance.getHazelcastInstance());
        }
        for (int i = firstHalfSize; i < instances.length; i++) {
            JetInstance currentInstance = instances[i];
            assertClusterSizeEventually(instances.length - firstHalfSize, currentInstance.getHazelcastInstance());
        }
    }

    private void closeExistingConnections() {
        applyOnBrains(CLOSE_CONNECTION);
    }

    private void blockCommunications() {
        applyOnBrains(BLOCK_COMMUNICATION);
    }

    private void healSplitBrain() {
        unblockCommunication();
        if (unblacklistHint) {
            unblacklistMembers();
        }
        if (shouldAssertAllNodesRejoined()) {
            for (JetInstance instance : instances) {
                assertClusterSizeEventually(instances.length, instance.getHazelcastInstance());
            }
        }
        waitAllForSafeState(Stream.of(instances).map(JetInstance::getHazelcastInstance).collect(Collectors.toList()));
    }

    private void unblockCommunication() {
        applyOnBrains(UNBLOCK_COMMUNICATION);
    }

    private void unblacklistMembers() {
        applyOnBrains(UNBLACKLIST_MEMBERS);
    }

    private static FirewallingConnectionManager getFireWalledConnectionManager(HazelcastInstance hz) {
        return (FirewallingConnectionManager) getNode(hz).getConnectionManager();
    }

    protected Brains getBrains() {
        int firstHalfSize = brains[0];
        int secondHalfSize = brains[1];
        JetInstance[] firstHalf = new JetInstance[firstHalfSize];
        JetInstance[] secondHalf = new JetInstance[secondHalfSize];
        for (int i = 0; i < instances.length; i++) {
            if (i < firstHalfSize) {
                firstHalf[i] = instances[i];
            } else {
                secondHalf[i - firstHalfSize] = instances[i];
            }
        }
        return new Brains(firstHalf, secondHalf);
    }

    private void applyOnBrains(SplitBrainAction action) {
        int firstHalfSize = brains[0];
        for (int isolatedIndex = 0; isolatedIndex < firstHalfSize; isolatedIndex++) {
            JetInstance isolatedInstance = instances[isolatedIndex];
            // do not take into account instances which have been shutdown
            if (!isInstanceActive(isolatedInstance)) {
                continue;
            }
            for (int i = firstHalfSize; i < instances.length; i++) {
                JetInstance currentInstance = instances[i];
                if (!isInstanceActive(currentInstance)) {
                    continue;
                }
                action.apply(isolatedInstance.getHazelcastInstance(), currentInstance.getHazelcastInstance());
            }
        }
    }

    private static boolean isInstanceActive(JetInstance instance) {
        if (instance.getHazelcastInstance() instanceof HazelcastInstanceProxy) {
            try {
                ((HazelcastInstanceProxy) instance.getHazelcastInstance()).getOriginal();
                return true;
            } catch (HazelcastInstanceNotActiveException exception) {
                return false;
            }
        } else if (instance.getHazelcastInstance() instanceof HazelcastInstanceImpl) {
            return getNode(instance.getHazelcastInstance()).getState() == NodeState.ACTIVE;
        } else {
            throw new AssertionError("Unsupported HazelcastInstance type");
        }
    }

    private static void unblacklistJoinerBetween(HazelcastInstance h1, HazelcastInstance h2) {
        Node h1Node = getNode(h1);
        Node h2Node = getNode(h2);
        h1Node.getJoiner().unblacklist(h2Node.getThisAddress());
        h2Node.getJoiner().unblacklist(h1Node.getThisAddress());
    }

    private interface SplitBrainAction {
        void apply(HazelcastInstance h1, HazelcastInstance h2);
    }

    static final class Brains {
        private final JetInstance[] firstHalf;
        private final JetInstance[] secondHalf;

        private Brains(JetInstance[] firstHalf, JetInstance[] secondHalf) {
            this.firstHalf = firstHalf;
            this.secondHalf = secondHalf;
        }

        JetInstance[] getFirstHalf() {
            return firstHalf;
        }

        JetInstance[] getSecondHalf() {
            return secondHalf;
        }
    }
}

