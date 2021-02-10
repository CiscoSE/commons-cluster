package com.cisco.commons.cluster.controller;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import lombok.extern.slf4j.Slf4j;

/**
 * ClusterController test.
 * @author Liran
 */
@Slf4j
public class ClusterControllerParallelTest {

    private static final TimeUnit DELAY_TIME_UNIT = TimeUnit.SECONDS;
    private static final int DELAY_TIME = 2;

    @Test
    public void clusterTest() {
        log.info("ClusterController test begin.");
        AtomicInteger leaderCount = new AtomicInteger();
        AtomicInteger instancesCountOnLeaderElected = new AtomicInteger();
        AtomicInteger stateChangedCount = new AtomicInteger();
        AtomicInteger messagesReceivedCount = new AtomicInteger();

        List<ClusterController> clusterControllers = new LinkedList<>();

        log.info("Creating testing embedded in-process ZooKeeper server");
        try (TestingServer zooKeeper = new TestingServer(true)) {
            log.info("Zookeeper server started.");

            String appInstance0 = "test-instance-0";
            String appInstance1 = "test-instance-1";

            try {
                String port = String.valueOf(zooKeeper.getPort());
                final ClusterController clusterController0 = newclusterController(null, port);
                clusterControllers.add(clusterController0);

                ClusterEventListener eventListener0 = new ClusterEventListener() {

                    @Override
                    public void takeLeadership() {
                        log.info("Consumer takeLeadership");
                        try {
                            int instancesCount = clusterController0.getClusterMembers().size();
                            log.info("Consumer takeLeadership instances size: {}", instancesCount);
                            instancesCountOnLeaderElected.set(instancesCount);
                            leaderCount.incrementAndGet();
                            clusterController0.setEventListener(this);
                        } catch (Exception e) {
                            log.error("Error: " + e.getMessage(), e);
                        }
                    }

                    @Override
                    public void stateChanged() {
                        log.info("Consumer stateChanged");
                        stateChangedCount.incrementAndGet();
                    }

                    @Override
                    public void onMessage(String message) {
                        log.info("Consumer {} got message: {}", appInstance0, message);
                        messagesReceivedCount.incrementAndGet();
                    }
                };

                clusterController0.setEventListener(eventListener0);

                clusterController0.setHost(appInstance0);
                log.info("Instance: {} joining cluster.", appInstance0);
                clusterController0.joinCluster();

                final ClusterController clusterController1 = newclusterController(null, port);
                clusterControllers.add(clusterController1);

                ClusterEventListener eventListener1 = new ClusterEventListener() {

                    @Override
                    public void takeLeadership() {
                        log.info("Consumer takeLeadership");
                        try {
                            int instancesCount = clusterController1.getClusterMembers().size();
                            log.info("Consumer takeLeadership instances size: {}", instancesCount);
                            instancesCountOnLeaderElected.set(instancesCount);
                            leaderCount.incrementAndGet();
                            clusterController0.setEventListener(eventListener0);
                            clusterController1.setEventListener(this);
                        } catch (Exception e) {
                            log.error("Error: " + e.getMessage(), e);
                        }
                    }

                    @Override
                    public void stateChanged() {
                        log.info("Consumer stateChanged");
                        stateChangedCount.incrementAndGet();
                    }

                    @Override
                    public void onMessage(String message) {
                        log.info("Consumer {} got message: {}", appInstance1, message);
                        messagesReceivedCount.incrementAndGet();
                    }
                };

                clusterController1.setEventListener(eventListener1);

                clusterController1.setHost(appInstance1);
                log.info("Instance: {} joining cluster.", appInstance1);
                clusterController1.joinCluster();

                String targetHost = null;
                log.info("instances:");
                List<ClusterMember> instances = clusterController0.getClusterMembers();
                for (ClusterMember clusterMember : instances) {
                    log.info("clusterMember: {}", clusterMember);
                    if (appInstance1.equals(clusterMember.getMemberName())) {
                        targetHost = clusterMember.getMemberName();
                    }
                    targetHost = clusterMember.getMemberName();
                }

                log.info("sending message");
                InstanceMessage instanceDetails = new InstanceMessage();
                instanceDetails.setTargetHost(targetHost);
                instanceDetails.setData(clusterController0.getClusterMember().getMemberName() + " sent message to " + targetHost);
                clusterController0.sendMessage(instanceDetails);

                Thread.sleep(2500);

                /*
                 * We expect events:
                 * - Leader created
                 */
                Assert.assertEquals(1, leaderCount.intValue());

                /*
                 * Test case is for consumer to be able to know about all cluster nodes when two nodes join together
                 * at the same time.
                 * We expect one of the cases:
                 * - Follower added
                 *   In this case, takeLeadership() would have only its self instance, then it will be followed by
                 *   follower added event, in this case it will know about it.
                 * - takeLeadership() would have the 2 instances already ready:
                 *   when 2 instances join in short latency, cluster state event change
                 *   by stateChanged() may not occur, but instead, the takeLeadership() would have
                 *   the 2 instances already ready.
                 */
                log.info("stateChangedCount: {}, instancesCountOnLeaderElected: {}", stateChangedCount, instancesCountOnLeaderElected);
                Assert.assertTrue(stateChangedCount.intValue() == 1 || instancesCountOnLeaderElected.get() == 2);

                log.info("instances:");
                instances = clusterController0.getClusterMembers();
                Assert.assertEquals(2, instances.size());

                Assert.assertEquals(1, messagesReceivedCount.intValue());

                log.info("leader instances paths: {}", clusterController0.getLeaderInstances());

                log.info("Simulating leader down");
                for (ClusterMember clusterMember : instances) {
                    log.info("clusterMember: {}", clusterMember);
                    if (appInstance0.equals(clusterMember.getMemberName()) && clusterMember.isLeader()) {
                        clusterController0.shutdown();
                        instances = clusterController1.getClusterMembers();
                        log.info("instances1: {}", instances);
                    } else if (appInstance1.equals(clusterMember.getMemberName()) && clusterMember.isLeader()) {
                        clusterController1.shutdown();
                        instances = clusterController0.getClusterMembers();
                        log.info("instances0: {}", instances);
                    }
                }

                Thread.sleep(1000);

                /*
                 * We expect events:
                 * - Leader created
                 * - Leader created after first leader went down
                 */
                Assert.assertEquals(2, leaderCount.intValue());

                Assert.assertEquals(1, instances.size());
            } finally {
                for (ClusterController clusterController : clusterControllers) {
                    if (clusterController != null) {
                        clusterController.shutdown();
                    }
                }
            }

            log.info("Test succeed.");

        } catch (Exception e) {
            log.error("Error: " + e.getMessage(), e);
            Assert.fail();
        }
    }

    private ClusterController newclusterController(ClusterEventListener eventListener, String port) {
        ClusterController clusterController = Mockito.spy(ClusterController.class);
        clusterController.setEventListener(eventListener);
        clusterController.setAppId("test-app");
        clusterController.setZkHost("localhost");
        clusterController.setZkPort(port);
        ClusterEventScheduler clusterEventScheduler = new ClusterEventScheduler(eventListener);
        clusterEventScheduler.setDelayTime(DELAY_TIME, DELAY_TIME_UNIT);
        Mockito.when(clusterController.createEventScheduler()).thenReturn(clusterEventScheduler);
        return clusterController;
    }
}
