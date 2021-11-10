package com.cisco.commons.cluster.controller.etcd;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Supplier;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ETCDConnection {

	private static final long DEFAULT_CONNECT_TIMEOUT_SECONDS = 3;
	private static final String TEST_HEALTH_KEY = "commons-cluster.test.health.key";
	
	@Getter @Setter(AccessLevel.PROTECTED)
    private long connectTimeoutSeconds = DEFAULT_CONNECT_TIMEOUT_SECONDS;
	
	@NonNull @Getter @Setter(AccessLevel.PROTECTED)
    private Collection<String> endpoints;
    
    @NonNull @Getter @Setter(AccessLevel.PROTECTED)
    private String etcdUser;
    
    private Supplier<String> etcPasswordSupplier;
    
    private Client client;
	
	@Getter
    private KV kvClient;
	
	@Getter
	private Lease leaseClient;
	
	@Builder
    private ETCDConnection(Collection<String> endpoints, long connectTimeoutSeconds, String etcdUser, Supplier<String> etcPasswordSupplier) {
        log.info("constructing");
        this.endpoints = endpoints;
        this.connectTimeoutSeconds = connectTimeoutSeconds;
        this.etcdUser = etcdUser;
        this.etcPasswordSupplier = etcPasswordSupplier;
    }
	
	public void connect() {
		
		// TODO add condition alreadyinit
		
//		connectivityCheckPool = Executors
//			  .newSingleThreadScheduledExecutor();
//		etcdPersistencePool = Executors.newFixedThreadPool(PERSISTENCE_POOL_SIZE);
		
		ClientBuilder clientBuilder = Client.builder().endpoints(endpoints.toArray(new String[endpoints.size()]))
    		.connectTimeout(Duration.ofSeconds(connectTimeoutSeconds));
		
		if (etcdUser != null) {
			clientBuilder.user(ETCDUtils.from(etcdUser)).password(ETCDUtils.from(etcPasswordSupplier.get()));
		}
		
		client = clientBuilder.build();
        kvClient = client.getKVClient();
        leaseClient = client.getLeaseClient();
        
        // TODO periodic check status and reconnect if needed
        
//        connectivityCheckPool.schedule(callable, delay, unit)
	}
	
	public boolean checkConnectivity() {
		try {
			ETCDUtils.get(TEST_HEALTH_KEY, kvClient, 3);
			return true;
		} catch (Exception e) {
			log.error("Error checkConnectivityAndUpdateStatus: " + e.getMessage(), e);
			return false;
		}
	}
	
	public void reconnect(String password) {
		
		// Not locking, since running requests expected to fail anyway.
		closeClientsQuitely();
		connect();
		
		boolean success = checkConnectivity();
		if (success) {
			log.info("ETCD connection status refreshed successfully.");
		} else {
			log.info("ETCD connection status refresh failed.");
		}
	}
	
	public void close() {
        log.info("close");
//        shouldRun.set(false);
        closeClientsQuitely();
    }
	
	private void closeClientsQuitely() {
		if (kvClient != null) {
            try {
                kvClient.close();
            } catch (Exception e) {
                log.error("Error closing kvClient: " + e.getMessage(), e);
            }
        }
		if (leaseClient != null) {
			try {
				leaseClient.close();
            } catch (Exception e) {
                log.error("Error closing leaseClient: " + e.getMessage(), e);
            }
		}
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.error("Error closing client: " + e.getMessage(), e);
            }
        }
	}
}
