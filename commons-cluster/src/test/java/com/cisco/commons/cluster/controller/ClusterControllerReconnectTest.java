package com.cisco.commons.cluster.controller;

import java.util.concurrent.CountDownLatch;
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
public class ClusterControllerReconnectTest {

    private static final TimeUnit DELAY_TIME_UNIT = TimeUnit.SECONDS;
    private static final int DELAY_TIME = 2;

    @Test
    public void clusterTest() {
        log.info("ClusterController test begin.");
        AtomicInteger leaderCount = new AtomicInteger();
        CountDownLatch leaderElectedLatch = new CountDownLatch(1);
        AtomicInteger stateChangedCount = new AtomicInteger();
        log.info("Creating testing embedded in-process ZooKeeper server");
        try (TestingServer zooKeeper = new TestingServer(false)) {
            log.info("Zookeeper server created.");
            String appInstance0 = "test-instance-0";
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
                }
            };

            ClusterController leaderClusterController = null;
            String port = String.valueOf(zooKeeper.getPort());

            try {
                leaderClusterController = newClusterController(eventListener0, port);
                leaderClusterController.setHost(appInstance0);
                final ClusterController currentClusterController = leaderClusterController;
                log.info("Instance: {} joining cluster while ZooKeeper is not available, before ZooKeeper server started.", appInstance0);
                
                Thread joinClusterTask = new Thread() {
                	@Override
                    public void run() {
                		try {
							currentClusterController.joinCluster();
							log.info("joined cluster.");
						} catch (Exception e) {
							log.error("Error joinCluster: " + e.getMessage(), e);
						}
                    }
                };
				joinClusterTask.start();
                log.info("Starting ZooKeeper server");
                zooKeeper.start();
                
                log.info("Waiting for join cluster to end");
                joinClusterTask.join(30000);
                log.info("Waiting for leader election");
                leaderElectedLatch.await(10, TimeUnit.SECONDS);
                joinClusterTask.interrupt();
                
                if (!leaderClusterController.getClusterMember().isLeader()) {
                	StackTraceElement[] stackTrace = joinClusterTask.getStackTrace();
                	log.info("stacktrace size: " + stackTrace.length);
                	for (StackTraceElement traceElement : stackTrace) {
                		log.info("stacktrace element: " + traceElement);
                	}
                }
                
                Assert.assertTrue(leaderClusterController.getClusterMember().isLeader());
                String appInstance0Path = "/" + ClusterController.PATH_PREFIX + "/service_discovery/test-app/" + appInstance0;
                boolean appInstance0PathExists = leaderClusterController.getClient()
                        .getZookeeperClient().getZooKeeper().exists(appInstance0Path, false) != null;
                log.info("Instance: {} exists ? : {}", appInstance0Path, appInstance0PathExists);
                Assert.assertTrue(appInstance0PathExists);
                
            } finally {
                log.info("Closing clients.");
                if (leaderClusterController != null) {
                    leaderClusterController.shutdown();
                }
            }
            
        } catch (Exception e) {
            log.error("Error: " + e.getMessage(), e);
            Assert.fail();
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
}
