package com.cisco.commons.cluster.controller;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import lombok.extern.slf4j.Slf4j;

/**
 * ClusterController test.
 * @author Liran
 */
@Slf4j
public class ClusterControllerTest {

    private static final TimeUnit DELAY_TIME_UNIT = TimeUnit.SECONDS;
    private static final int DELAY_TIME = 2;

    @Test
    public void clusterTest() {
        log.info("ClusterController test begin.");
        AtomicInteger leaderCount = new AtomicInteger();
        CountDownLatch leaderElectedLatch = new CountDownLatch(1);
        AtomicInteger stateChangedCount = new AtomicInteger();
        AtomicInteger messagesReceivedCount = new AtomicInteger();
        AtomicBoolean isLeadershipLostCalled = new AtomicBoolean(false);
        ClusterController connectionLostTestClusterController = null;
        log.info("Creating testing embedded in-process ZooKeeper server");
        try (TestingServer zooKeeper = new TestingServer(true)) {
            log.info("Zookeeper server started.");

            String appInstance0 = "test-instance-0";
            String appInstance1 = "test-instance-1";

            AtomicBoolean messageHostCorrect = new AtomicBoolean(true);

            ClusterEventListener eventListener0 = new ClusterEventListener() {

                @Override
                public void takeLeadership() {
                    log.info("Consumer takeLeadership");
                    leaderCount.incrementAndGet();
                    leaderElectedLatch.countDown();
                }

                @Override
                public void stateChanged() {
                    log.info("Consumer stateChanged");
                    stateChangedCount.incrementAndGet();
                }

                @Override
                public void onMessage(String message) {
                    log.info("Consumer {} got message: {}", appInstance0, message);
                    if (!message.endsWith(appInstance0)) {
                        messageHostCorrect.set(false);
                    }
                    messagesReceivedCount.incrementAndGet();
                }
            };

            ClusterEventListener eventListener1 = new ClusterEventListener() {

                @Override
                public void takeLeadership() {
                    log.info("Consumer takeLeadership");
                    leaderCount.incrementAndGet();
                }

                @Override
                public void stateChanged() {
                    log.info("Consumer stateChanged");
                    stateChangedCount.incrementAndGet();
                }

                @Override
                public void onMessage(String message) {
                    log.info("Consumer {} got message: {}", appInstance1, message);
                    if (!message.endsWith(appInstance1)) {
                        messageHostCorrect.set(false);
                    }
                    messagesReceivedCount.incrementAndGet();
                }
            };

            ClusterController leaderClusterController = null;
            ClusterController followerClusterController = null;
            String port = String.valueOf(zooKeeper.getPort());

            try {
                leaderClusterController = newClusterController(eventListener0, port);
                leaderClusterController.setHost(appInstance0);
                log.info("Instance: {} joining cluster.", appInstance0);
                leaderClusterController.joinCluster();
                leaderElectedLatch.await(10, TimeUnit.SECONDS);
                Assert.assertTrue(leaderClusterController.getClusterMember().isLeader());
                
                String appInstance0Path = "/" + ClusterController.PATH_PREFIX + "/service_discovery/test-app/" + appInstance0;
                boolean appInstance0PathExists = leaderClusterController.getClient()
                        .getZookeeperClient().getZooKeeper().exists(appInstance0Path, false) != null;
                log.info("Instance: {} exists ? : {}", appInstance0Path, appInstance0PathExists);
                Assert.assertTrue(appInstance0PathExists);

                /*
                 * We expect events:
                 * - Leader created
                 */
                Assert.assertEquals(1, leaderCount.intValue());

                followerClusterController = newClusterController(eventListener1, port);
                followerClusterController.setHost(appInstance1);
                log.info("Instance: {} joining cluster.", appInstance1);
                followerClusterController.joinCluster();

                String targetHost = null;
                List<ClusterMember> instances = leaderClusterController.getClusterMembers();
                
                logAllNodesPaths(followerClusterController.getClient().getZookeeperClient().getZooKeeper(), "/");
                
                validateMembers(instances, Arrays.asList(appInstance0, appInstance1));
                for (ClusterMember clusterMember : instances) {
                    log.info("clusterMember: {}", clusterMember);
                    if (appInstance1.equals(clusterMember.getMemberName())) {
                        targetHost = clusterMember.getMemberName();
                    }
                    targetHost = clusterMember.getMemberName();
                }

                log.info("sending message");
                InstanceMessage instanceDetails = new InstanceMessage();
                log.info("source host: {}", leaderClusterController.getClusterMember().getMemberName());
                instanceDetails.setSourceHost(leaderClusterController.getClusterMember().getMemberName());
                instanceDetails.setTargetHost(targetHost);
                instanceDetails.setData(leaderClusterController.getClusterMember().getMemberName() + " sent message to " + targetHost);
                leaderClusterController.sendMessage(instanceDetails);

                logAllNodesPaths(followerClusterController.getClient().getZookeeperClient().getZooKeeper(), "/" + ClusterController.PATH_PREFIX + "/service_discovery");
                
                Thread.sleep(1000);
                log.info("sending message 2");
                InstanceMessage instanceDetails2 = new InstanceMessage();
                targetHost = appInstance0;
                instanceDetails2.setTargetHost(targetHost);
                instanceDetails2.setData(followerClusterController.getClusterMember().getMemberName() + " sent message to " + targetHost);
                followerClusterController.sendMessage(instanceDetails2);

                Thread.sleep(1000);

                log.info("sending message 3 (changed message, same source and target)");
                instanceDetails2 = new InstanceMessage();
                targetHost = appInstance0;
                instanceDetails2.setTargetHost(targetHost);
                instanceDetails2.setData("3 " + followerClusterController.getClusterMember().getMemberName() + " sent message to " + targetHost);
                followerClusterController.sendMessage(instanceDetails2);

                Thread.sleep(1000);

                logAllNodesPaths(followerClusterController.getClient().getZookeeperClient().getZooKeeper(), "/" + ClusterController.PATH_PREFIX + "/service_discovery");
                instances = leaderClusterController.getClusterMembers();
                validateMembers(instances, Arrays.asList(appInstance0, appInstance1));
                log.info("leader instances: {}", instances);
                instances = followerClusterController.getClusterMembers();
                log.info("follower instances: {}", instances);
                validateMembers(instances, Arrays.asList(appInstance0, appInstance1));
                for (ClusterMember clusterMember : instances) {
                    if (appInstance0.equals(clusterMember.getMemberName())) {
                        Assert.assertEquals(true, clusterMember.isLeader());
                        Assert.assertEquals(true, leaderClusterController.getClusterMember().isLeader());
                    } else {
                        Assert.assertEquals(false, clusterMember.isLeader());
                    }
                }
                Assert.assertEquals(2, instances.size());

                /*
                 * We expect events:
                 * - Follower added
                 */
                Assert.assertEquals(1, stateChangedCount.intValue());

                Assert.assertTrue(messageHostCorrect.get());

                Assert.assertEquals(3, messagesReceivedCount.intValue());

                log.info("leader instances paths: {}", leaderClusterController.getLeaderInstances());
                
                logAllNodesPaths(followerClusterController.getClient().getZookeeperClient().getZooKeeper(), "/");

                log.info("Simulating leader down");
                leaderClusterController.shutdown();

                Thread.sleep(1000);

                /*
                 * We expect events:
                 * - Leader created
                 * - Leader created after first leader went down
                 */
                Assert.assertEquals(2, leaderCount.intValue());

                /*
                 * We expect events:
                 * - Follower added
                 */
                Assert.assertEquals(1, stateChangedCount.intValue());

                log.info("instances:");
                instances = followerClusterController.getClusterMembers();
                validateMembers(instances, Arrays.asList(appInstance1));
                for (ClusterMember clusterMember : instances) {
                    log.info("clusterMember: {}", clusterMember);
                    if (appInstance1.equals(clusterMember.getMemberName())) {
                        Assert.assertEquals(true, clusterMember.isLeader());
                    }
                }
                Assert.assertEquals(1, instances.size());

                // follower (who was the leader at the beginning)
                leaderClusterController = newClusterController(eventListener0, port);
                leaderClusterController.setHost(appInstance0);
                log.info("Instance: {} joining cluster.", appInstance0);
                leaderClusterController.joinCluster();

                // shutdown of follower which has just joined
                Thread.sleep(1000);
                log.info("Simulating follower down");
                leaderClusterController.shutdown();

                Thread.sleep(500);

                /*
                 * We expect events:
                 * - Follower added from before
                 * - 1 stateChanged event (instead of 2) for both calls of join+shutdown in less than the configured delay time of 2 seconds.
                 */
                Assert.assertEquals(1, stateChangedCount.intValue());

                Thread.sleep(1000);

                /*
                 * We expect events:
                 * - Follower added from before
                 * - 2 stateChanged events for both calls of join+shutdown after the configured delay time of 2 seconds.
                 */
                Assert.assertEquals(2, stateChangedCount.intValue());
                
            } finally {
                log.info("Closing clients.");
                if (followerClusterController != null) {
                    followerClusterController.shutdown();
                }
                if (leaderClusterController != null) {
                    leaderClusterController.shutdown();
                }
            }
            
            ClusterEventListener connectionLostEventListener = new ClusterEventListener() {
                
                @Override
                public void takeLeadership() {
                    log.info("connectionLostEventListener takeLeadership");
                }
                
                @Override
                public void stateChanged() {
                    log.info("connectionLostEventListener stateChanged");
                }
                
                @Override
                public void onMessage(String message) {
                    log.info("connectionLostEventListener onMessage: " + message);
                }
                
                @Override 
                public void leadershipLost() {
                    log.info("connectionLostEventListener leadershipLost");
                    isLeadershipLostCalled.set(true);
                }
                
            };
            connectionLostTestClusterController = newClusterController(connectionLostEventListener , port);
            String instance = "connectionLostTest";
            connectionLostTestClusterController.setHost(instance);
            log.info("Instance: {} joining cluster.", instance);
            connectionLostTestClusterController.joinCluster();

            Thread.sleep(1000);

            Assert.assertTrue(connectionLostTestClusterController.getClusterMember().isLeader());

        } catch (Exception e) {
            log.error("Error: " + e.getMessage(), e);
            Assert.fail();
        }

        try {
            
            Thread.sleep(1000);
            
            Assert.assertTrue(isLeadershipLostCalled.get());
            
            log.info("getClusterMembers of connectionLostTestClusterController");
            List<ClusterMember> instances = connectionLostTestClusterController.getClusterMembers();
            Assert.assertTrue(instances.isEmpty());
            
            log.info("Test succeed.");
        } catch (Exception e) {
            log.error("Error connectionLostTestClusterController: " + e.getMessage(), e);
            Assert.fail();
        } finally {
            log.info("Shutting down connectionLostTestClusterController.");
            if (connectionLostTestClusterController != null) {
                connectionLostTestClusterController.shutdown();
            }
        }
    }
    
    private void logAllNodesPaths(ZooKeeper zooKeeperClient, String path) throws KeeperException, InterruptedException, Exception {
        log.info("logAllNodesPaths called for path: {}", path);
        List<String> children = zooKeeperClient.getChildren(path, false);
        log.info("ZooKeeper all children: {}", children);
        for (String childPath: children) {
            String newPath = childPath;
            if (path.equals("/")) {
                newPath = "/" + childPath;
            } else {
                newPath = path + "/" + childPath;
            }
            logAllNodesPaths(zooKeeperClient, newPath);
        }
    }

    private void validateMembers(List<ClusterMember> instances, Collection<String> hosts) {
        log.info("Validate instances: {} with hosts: {}", instances, hosts);
        Set<String> memberNames = new HashSet<>();
        for (ClusterMember clusterMember : instances) {
            memberNames.add(clusterMember.getMemberName());
        }
        for (String host: hosts) {
            log.info("Validate host: {}", host);
            assertTrue(memberNames.contains(host));
        }
    }

    private ClusterController newClusterController(ClusterEventListener eventListener, String port) {
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
    
    @Test
    public void clusterControllerBuilderTest() {
    	ClusterEventListener eventListener = null;
		String zkHost = null;
		String zkPort = null;
		String instanceHost = null;
		String appId = "example-appId";
		ClusterController clusterController = ClusterController.builder().appId(appId).eventListener(eventListener)
			.expectedNumberOfInstances(2).zkHost(zkHost).zkPort(zkPort).host(instanceHost).build();
		assertTrue(appId.equals(clusterController.getAppId()));
    }
}
