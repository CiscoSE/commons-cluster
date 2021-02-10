package com.cisco.commons.cluster.controller;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Provides capabilities for clustering features for managing multiple instances services like Service Discovery and Leader Election, subscribing to instance up/down events, and sending data to application instances.
 * The library is implemented with Curator on top of Apache ZooKeeper.
 * Main purpose is Leader Election algorithm, with eliminating the "herd" effect.
 * 
 * Basic usage includes calling joinCluster() and implementing ClusterEventListener.
 * shutdown() must be called at the end.
 * 
 * @author Liran Mendelovich
 * 
 * Copyright 2021 Cisco Systems
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@Slf4j
public class ClusterController {

    protected static final String PATH_PREFIX = "commons-cluster";
    private static final String SERVICE_DISCOVERY = "service_discovery";
    private static final String LEADER_PATH_PREFIX = "leader_election_nodes";
    private String serviceDiscoveryPath;
    private String instancePrefix;
    
    @NonNull @Getter @Setter(AccessLevel.PROTECTED)
    private String appId;
    
    @NonNull @Getter @Setter(AccessLevel.PROTECTED)
    private String host;
    
    @NonNull @Getter @Setter(AccessLevel.PROTECTED)
    private String zkHost;
    
    @NonNull @Getter @Setter(AccessLevel.PROTECTED)
    private String zkPort;
    
    @NonNull @Getter @Setter(AccessLevel.PROTECTED)
    private Integer expectedNumberOfInstances;
    
    @Getter @Setter(AccessLevel.PROTECTED)
    private boolean isUseGracePeriod = true;
    
    @Getter
    private CuratorFramework client;
    
    @Getter
    private ClusterMember clusterMember;
    
    @Getter
    private ClusterEventListener eventListener;
    
    private ServiceDiscovery<InstanceMessage> serviceDiscovery;
    private ServiceCache<InstanceMessage> serviceCache = null;
    private LeaderLatch leaderLatch;
    private String leaderPath;
    private PersistentWatcher persistentWatcher;
    private AtomicBoolean alreadyJoinedCluster = new AtomicBoolean();
    private AtomicBoolean alreadyShutDown = new AtomicBoolean();
    private List<String> instanceNames;
    private ExecutorService leaderListenerPool;
    private ExecutorService initPool;
    private AtomicBoolean shouldRun = new AtomicBoolean(true);
    private JsonInstanceSerializer<InstanceMessage> serializer;
    private ClusterEventScheduler eventScheduler;
    private ExecutorService messagesPool;

    @Builder
    private ClusterController(ClusterEventListener eventListener, String appId, int expectedNumberOfInstances, boolean isUseGracePeriod, String zkHost, String zkPort, String host) {
        log.info("init");
        this.eventListener = eventListener;
        this.appId = appId;
        this.expectedNumberOfInstances = expectedNumberOfInstances;
        this.isUseGracePeriod = isUseGracePeriod;
        this.zkHost = zkHost;
        this.zkPort = zkPort;
    }
    
    protected ClusterController() {
    	
    }

    /**
     * Join cluster as a new instance, and blocking until succeed.
     * Not supported to be called several times on same instance.
     * Uses internal retry mechanism for failures.
     */
    public void joinCluster() {
        if (alreadyJoinedCluster.getAndSet(true)) {
            String errorMessage = "Already tried joining the cluster.";
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
        log.info("joinCluster for appId: {}, instance host: {}", appId, host);
        serviceDiscoveryPath = ZKPaths.PATH_SEPARATOR + PATH_PREFIX + ZKPaths.PATH_SEPARATOR + SERVICE_DISCOVERY;
        instancePrefix = serviceDiscoveryPath + ZKPaths.PATH_SEPARATOR + appId;
        String fullPath = instancePrefix + ZKPaths.PATH_SEPARATOR + host;
        log.info("Joining cluster for path: {}", fullPath);
        clusterMember = new ClusterMember(appId, host, "localhost", fullPath, false);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(5000, 15);
        String zkConnectString = zkHost + ":" + zkPort;
        log.info("Connecting ZooKeeer url: {}", zkConnectString);
        log.info("clusterJoinTask begin");
        initPool = Executors.newSingleThreadExecutor();
        AtomicBoolean success = new AtomicBoolean(false);
        while (shouldRun.get() && !success.get()) {
            try {
            	
            	/*
            	 * It needs to be executed in a separate thread in order to interrupt it if it hangs.
            	 * From testing multiple times, on some executions where ZooKeeper is not available while
            	 * joining cluster, it got waiting and hanging indefinitely, probably due to a race condition.
            	 * For more details, see ClusterControllerReconnectTest and
            	 * docs/join_while_zookeeper_down_issue.txt
            	 */
                Callable<Boolean> joinClusterTask = () -> {
                	try {
                		log.info("Joining cluster.");
						joinClusterHelper(retryPolicy, zkConnectString);
						log.info("Joining cluster succeed.");
						success.set(true);
						return true;
                	} catch (Exception e) {
                        log.error("Error joining cluster task: " + e.getMessage(), e);
                        return false;
                    }
                };
                log.info("Executing join cluster task.");
				initPool.invokeAll(Arrays.asList(joinClusterTask), 30, TimeUnit.SECONDS);
				initPool.shutdownNow();
				initPool.awaitTermination(30, TimeUnit.SECONDS);
				log.info("Join cluster task done.");
            } catch (Exception e) {
                log.error("Error joining cluster: " + e.getMessage(), e);
                close();
                log.info("Delaying, and will try joining cluster again.");
                sleepQuitely(30000);
            }
        }
        log.info("joinCluster end");
    }

	private void joinClusterHelper(RetryPolicy retryPolicy, String zkConnectString)
			throws Exception, InterruptedException {
		messagesPool = Executors.newFixedThreadPool(4);
        leaderListenerPool = Executors.newSingleThreadExecutor();
		client = CuratorFrameworkFactory.newClient(zkConnectString, 60000, 30000, retryPolicy);
        client.start();
        serializer = new JsonInstanceSerializer<InstanceMessage>(InstanceMessage.class);
        InstanceMessage instanceDetails = new InstanceMessage();
        instanceDetails.setSourceHost(host);
        instanceDetails.setTargetHost(host);
        ServiceInstance<InstanceMessage> serviceInstance = ServiceInstance.<InstanceMessage>builder()
                .payload(instanceDetails).name(appId).id(host).build();
        serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceMessage.class)
                .basePath(serviceDiscoveryPath).serializer(serializer).client(client)
                .watchInstances(true).thisInstance(serviceInstance).build();
        log.info("Service Discovery starting: {}", serviceDiscoveryPath);
        serviceDiscovery.start();
        subscribeForMessages();
        leaderPath = ZKPaths.PATH_SEPARATOR + PATH_PREFIX + ZKPaths.PATH_SEPARATOR + LEADER_PATH_PREFIX + ZKPaths.PATH_SEPARATOR + this.appId;
        log.info("starting leaderLatch: " + leaderPath);
        leaderLatch = new LeaderLatch(client, leaderPath, host);
        leaderLatch.addListener(createLeaderListener(), leaderListenerPool);
        leaderLatch.start();
        Participant leader = leaderLatch.getLeader();
        String leaderId = null;
        if (leader != null) {
            leaderId = leader.getId();
        }
        log.info("leader: {}", leaderId);
	}

    private void sleepQuitely(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            log.debug("Error sleeping: {}", e.getMessage());
        }
    }

    private void delete(String path) {
        try {
            client.getZookeeperClient().getZooKeeper().delete(path, -1);
        } catch (Exception e) {
            log.info("Could not delete {}: {}", path, e.getMessage());
        }
    }

    private LeaderLatchListener createLeaderListener() {
        return new LeaderLatchListener() {

            /**
             * we are now the leader. This method returns when leaderLatch gets closed in shutdown
             */
            @Override
            public void isLeader() {
                try {
                    log.info(host + " is leader");
                    clusterMember.setLeader(true);
                    serviceCache = serviceDiscovery.serviceCacheBuilder().name(appId).build();
                    log.info(host + " service discovery cache starting");
                    serviceCache.start();
                    addCacheListener();
                    eventScheduler = createEventScheduler();
                    instanceNames = getSortedInstances();
                    log.info("Triggering eventListener takeLeadership");
                    eventListener.takeLeadership();
                } catch (Exception e) {
                    log.error("Error on starting cache: " + e.getMessage(), e);
                }
            }

            /**
             * this method is called only when leaderLatch is closed with "close(CloseMode.NOTIFY_LEADER)"
             */
            @Override
            public void notLeader() {
                log.info("notLeader");
                clusterMember.setLeader(false);
                if (serviceCache != null) {
                    closeQuietly(serviceCache);
                }
                if (eventScheduler != null) {
                    eventScheduler.shutdown();
                }
                log.info("Triggering eventListener leadershipLost");
                eventListener.leadershipLost();
            }
        };
    }

    protected ClusterEventScheduler createEventScheduler() {
        return new ClusterEventScheduler(eventListener);
    }

    private void addCacheListener() {
        serviceCache.addListener(new ServiceCacheListener() {

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                try {
                    log.debug(" listener State changed to: " + newState);
                } catch (Exception e) {
                    log.error("Error state: " + e.getMessage(), e);
                }
            }

            @Override
            public void cacheChanged() {
                try {
                    List<String> prevInstanceNames = instanceNames;
                    List<String> instances = getSortedInstances();
                    log.info("cacheChanged host: {} instances: {}, prev instanceNames: {}", host, instances, prevInstanceNames);
                    if (instances.equals(instanceNames)) {
                        log.debug("Filtering change, as instances not changed.");
                        return;
                    } else {
                        instanceNames = instances;
                    }
                    log.info("Cache changed, instances size: " + instances.size());
                    if (expectedNumberOfInstances != null && expectedNumberOfInstances > 0 && instances.size() == expectedNumberOfInstances) {
                        log.info("Reached to the expected number of instances: {}. Calling listener's stateChanged.", expectedNumberOfInstances);
                        eventListener.stateChanged();
                    } else {
                        if (isUseGracePeriod) {
                            log.info("Using grace period");
                            eventScheduler.scheduleEvent();
                        } else {
                            log.info("Calling listener's stateChanged.");
                            eventListener.stateChanged();
                        }
                    }
                } catch (Exception e) {
                    log.error("Error cacheChanged: " + e.getMessage(), e);
                }
            }
        });
    }

    private List<String> getSortedInstances() throws Exception {
        return ZKPaths.getSortedChildren(client.getZookeeperClient().getZooKeeper(), instancePrefix);
    }

    public boolean isLeader() {
        return leaderLatch != null ? leaderLatch.hasLeadership() : false;
    }

    protected void setEventListener(ClusterEventListener eventListener) {
        this.eventListener = eventListener;
        if (eventScheduler != null) {
            this.eventScheduler.setEventListener(eventListener);
        }
    }

    public List<ClusterMember> getClusterMembers() throws Exception {
        return getClusterMembers(false);
    }

    public List<ClusterMember> getClusterMembers(boolean useCache) throws Exception {
        if (!this.alreadyJoinedCluster.get()) {
            String errorMessage = "Did not joined this cluster yet";
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        
        if (!client.getZookeeperClient().isConnected()) {
            log.warn("ZooKeeper client is not connected");
            return Collections.emptyList();
        }

        String leaderId = null;
        Participant leader = leaderLatch.getLeader();
        if (leader != null) {
            leaderId = leader.getId();
        }
        log.info("leaderId: " + leaderId);
        List<ClusterMember> members = new ArrayList<>();
        Collection<ServiceInstance<InstanceMessage>> serviceInstances = null;
        if (useCache && serviceCache != null && isLeader()) {
            log.info("Taking serviceInstances from cache.");
            serviceInstances = serviceCache.getInstances();
        } else {
            serviceInstances = serviceDiscovery.queryForInstances(appId);
        }
        log.info("serviceInstances: {}", serviceInstances);
        for (ServiceInstance<InstanceMessage> serviceInstance : serviceInstances) {
            log.debug("serviceInstance: {}", serviceInstance);
            String fullPath = instancePrefix + ZKPaths.PATH_SEPARATOR + serviceInstance.getId();
            boolean isLeader = Objects.equals(serviceInstance.getId(), leaderId);
            ClusterMember member = new ClusterMember(appId, serviceInstance.getId(), serviceInstance.getAddress(), fullPath, isLeader);
            members.add(member);
        }
        return members;
    }

    protected List<String> getLeaderInstances() throws Exception {
        return ZKPaths.getSortedChildren(client.getZookeeperClient().getZooKeeper(), leaderPath);
    }

    public void sendMessage(InstanceMessage instanceMessage) throws Exception {
        try {
            String instancePath = instancePrefix + ZKPaths.PATH_SEPARATOR + instanceMessage.getTargetHost();
            log.info(host + " sending message to target instance: {} with message: {}", instancePath, instanceMessage);
            ServiceInstance<InstanceMessage> serviceInstance = ServiceInstance.<InstanceMessage>builder()
                    .payload(instanceMessage).name(appId).id(instanceMessage.getTargetHost()).build();
            byte[] serviceInstanceBytes = serializer.serialize(serviceInstance);
            Stat stat = client.setData().forPath(instancePath, serviceInstanceBytes);
            log.info("Sent message.");
            log.debug("Message status: {}", stat);
        } catch (Exception e) {
            log.error("Error sending data to instance: " + instanceMessage.getTargetHost() + " with message: " + instanceMessage + " error: " + e.getMessage(), e);
            throw e;
        }
    }

    private void subscribeForMessages() {
        log.info("Subscribing for messages");
        String instancePath = this.getClusterMember().getFullPath();
        persistentWatcher = new PersistentWatcher(client, instancePath, false);
        persistentWatcher.start();
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    log.info("Watcher processing message event for path: {}, state: {}, type: {}", event.getPath(), event.getState(), event.getType());

                    if (!shouldRun.get()) {
                        log.info("Cluster controller is closed.");
                        return;
                    }
                    
                    if (!EventType.NodeDataChanged.equals(event.getType()) || KeeperState.Closed.equals(event.getState())) {
                        log.info("Filtering irrelevant event.");
                        return;
                    }
                    
                    byte[] instanceBytes = client.getData().forPath(event.getPath());
                    ServiceInstance<InstanceMessage> serviceInstance = serializer.deserialize(instanceBytes);
                    InstanceMessage instanceDetails = serviceInstance.getPayload();
                    log.info("Watcher got message: {}", instanceDetails);
                    log.info("Calling event listener onMessage.");
                    eventListener.onMessage(instanceDetails.getData());
                } catch (Exception e) {
                    log.error("Error processing event: " + e.getMessage(), e);
                }
            }
        };
        persistentWatcher.getListenable().addListener(watcher, messagesPool);
    }

    protected void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            log.debug("Error sleeping: " + e.getMessage());
        }
    }

    private void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn("closeQuitely Got error: " + e.getMessage());
        }
    }

    /**
     * Shutdown the cluster controller.
     */
    public void shutdown() {
        if (this.alreadyShutDown.getAndSet(true)) {
            log.info("Cluster already shut down");
            return;
        }

        try {
            log.info("Shutting down {}", host);
            shouldRun.set(false);
            if (initPool != null) {
                initPool.shutdownNow();
            }
            close();
            log.info("Shut down {}", host);
        } catch (Exception e) {
            log.error("Shutdown Got error: " + e.getMessage(), e);
        }
    }

	private void close() {
		try {
			messagesPool.shutdownNow();
			closeQuietly(persistentWatcher);
			if (serviceCache != null) {
			    closeQuietly(serviceCache);
			}
			if (eventScheduler != null) {
			    eventScheduler.shutdown();
			}
			if (client != null && client.getZookeeperClient() != null && client.getZookeeperClient().isConnected()) {
			    
			    // When ZooKeeper is not connected (connection loss) - this is stuck, without timeout.
			    log.info("Unregistering service discovery");
			    serviceDiscovery.close();
			}
			delete(instancePrefix + ZKPaths.PATH_SEPARATOR + host);
			if (leaderLatch != null) {
			    closeQuietly(leaderLatch);
			}
			if (leaderListenerPool != null) {
			    leaderListenerPool.shutdownNow();
			}
			log.info("Closing client");
			closeQuietly(client);
		} catch (IOException e) {
			log.error("Error closing: " + e.getMessage(), e);
		}
	}

}
