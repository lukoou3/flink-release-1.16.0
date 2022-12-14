/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedLeaderService;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.TestLogger;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for leader election. */
@RunWith(Parameterized.class)
public class LeaderElectionTest extends TestLogger {

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    @Rule
    public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerResource();

    enum LeaderElectionType {
        ZooKeeper,
        Embedded,
        Standalone
    }

    @Parameterized.Parameters(name = "Leader election: {0}")
    public static Collection<LeaderElectionType> parameters() {
        return Arrays.asList(LeaderElectionType.values());
    }

    private final ServiceClass serviceClass;

    public LeaderElectionTest(LeaderElectionType leaderElectionType) {
        switch (leaderElectionType) {
            case ZooKeeper:
                serviceClass = new ZooKeeperServiceClass();
                break;
            case Embedded:
                serviceClass = new EmbeddedServiceClass();
                break;
            case Standalone:
                serviceClass = new StandaloneServiceClass();
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Unknown leader election type: %s.", leaderElectionType));
        }
    }

    @Before
    public void setup() throws Exception {
        serviceClass.setup(testingFatalErrorHandlerResource.getFatalErrorHandler());
    }

    @After
    public void teardown() throws Exception {
        serviceClass.teardown();
    }

    @Test
    public void testHasLeadership() throws Exception {
        final LeaderElectionService leaderElectionService =
                serviceClass.createLeaderElectionService();
        final ManualLeaderContender manualLeaderContender = new ManualLeaderContender();

        try {
            assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()), is(false));

            leaderElectionService.start(manualLeaderContender);

            final UUID leaderSessionId = manualLeaderContender.waitForLeaderSessionId();

            assertThat(leaderElectionService.hasLeadership(leaderSessionId), is(true));
            assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()), is(false));

            leaderElectionService.confirmLeadership(leaderSessionId, "foobar");

            assertThat(leaderElectionService.hasLeadership(leaderSessionId), is(true));

            leaderElectionService.stop();

            assertThat(leaderElectionService.hasLeadership(leaderSessionId), is(false));
        } finally {
            manualLeaderContender.rethrowError();
        }
    }

    private static final class ManualLeaderContender implements LeaderContender {

        private static final UUID NULL_LEADER_SESSION_ID = new UUID(0L, 0L);

        private final ArrayBlockingQueue<UUID> leaderSessionIds = new ArrayBlockingQueue<>(10);

        private volatile Exception exception;

        @Override
        public void grantLeadership(UUID leaderSessionID) {
            leaderSessionIds.offer(leaderSessionID);
        }

        @Override
        public void revokeLeadership() {
            leaderSessionIds.offer(NULL_LEADER_SESSION_ID);
        }

        @Override
        public String getDescription() {
            return "foobar";
        }

        @Override
        public void handleError(Exception exception) {
            this.exception = exception;
        }

        void rethrowError() throws Exception {
            if (exception != null) {
                throw exception;
            }
        }

        UUID waitForLeaderSessionId() throws InterruptedException {
            return leaderSessionIds.take();
        }
    }

    private interface ServiceClass {
        void setup(FatalErrorHandler fatalErrorHandler) throws Exception;

        void teardown() throws Exception;

        LeaderElectionService createLeaderElectionService() throws Exception;
    }

    private static final class ZooKeeperServiceClass implements ServiceClass {

        private TestingServer testingServer;

        private CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

        private Configuration configuration;

        @Override
        public void setup(FatalErrorHandler fatalErrorHandler) throws Exception {
            try {
                testingServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
            } catch (Exception e) {
                throw new RuntimeException("Could not start ZooKeeper testing cluster.", e);
            }

            configuration = new Configuration();

            configuration.setString(
                    HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
            configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

            curatorFrameworkWrapper =
                    ZooKeeperUtils.startCuratorFramework(configuration, fatalErrorHandler);
        }

        @Override
        public void teardown() throws Exception {
            if (curatorFrameworkWrapper != null) {
                curatorFrameworkWrapper.close();
                curatorFrameworkWrapper = null;
            }

            if (testingServer != null) {
                testingServer.close();
                testingServer = null;
            }
        }

        @Override
        public LeaderElectionService createLeaderElectionService() throws Exception {
            return ZooKeeperUtils.createLeaderElectionService(
                    curatorFrameworkWrapper.asCuratorFramework());
        }
    }

    private static final class EmbeddedServiceClass implements ServiceClass {
        private EmbeddedLeaderService embeddedLeaderService;

        @Override
        public void setup(FatalErrorHandler fatalErrorHandler) {
            embeddedLeaderService = new EmbeddedLeaderService(EXECUTOR_RESOURCE.getExecutor());
        }

        @Override
        public void teardown() {
            if (embeddedLeaderService != null) {
                embeddedLeaderService.shutdown();
                embeddedLeaderService = null;
            }
        }

        @Override
        public LeaderElectionService createLeaderElectionService() throws Exception {
            return embeddedLeaderService.createLeaderElectionService();
        }
    }

    private static final class StandaloneServiceClass implements ServiceClass {

        @Override
        public void setup(FatalErrorHandler fatalErrorHandler) throws Exception {
            // noop
        }

        @Override
        public void teardown() throws Exception {
            // noop
        }

        @Override
        public LeaderElectionService createLeaderElectionService() throws Exception {
            return new StandaloneLeaderElectionService();
        }
    }
}
