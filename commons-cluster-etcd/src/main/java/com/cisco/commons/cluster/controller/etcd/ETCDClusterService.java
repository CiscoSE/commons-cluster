package com.cisco.commons.cluster.controller.etcd;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.cisco.commons.cluster.controller.ClusterService;
import com.cisco.commons.cluster.controller.PersistencyListener;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.grpc.stub.StreamObserver;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ETCDClusterService implements ClusterService {

	
	private static final long DEFAULT_REQUEST_TIMEOUT_SECONDS = 3;
	
	private static final Charset CHARSET = StandardCharsets.UTF_8;
	
	@Getter @Setter(AccessLevel.PROTECTED)
    private long requestTimeoutSeconds = DEFAULT_REQUEST_TIMEOUT_SECONDS;
	
	@NonNull
	private ETCDConnection etcdConnection;
	
	private Map<String, Long> keyToLeaseId;
	
//	private ExecutorService etcdPersistencePool;
	
//	private ScheduledExecutorService connectivityCheckPool;
	
	@Builder
    private ETCDClusterService(String appId, String instanceId, int expectedNumberOfInstances, boolean isUseGracePeriod, 
    		long requestTimeoutSeconds, ETCDConnection etcdConnection) {
        log.info("constructing");
        this.requestTimeoutSeconds = requestTimeoutSeconds;
        this.etcdConnection = etcdConnection;
    }
	
	@Override
	public void init(String password) {
		
		// TODO add condition alreadyinit
		
//		connectivityCheckPool = Executors
//			  .newSingleThreadScheduledExecutor();
//		etcdPersistencePool = Executors.newFixedThreadPool(PERSISTENCE_POOL_SIZE);
		keyToLeaseId = new ConcurrentHashMap<>();
        
//        connectivityCheckPool.schedule(callable, delay, unit)
	}

	// TODO move methods to util class
	
	@Override
	public String get(String key) throws Exception {
		ByteSequence keyByteSequence = from(key);
        CompletableFuture<GetResponse> getFuture = etcdConnection.getKvClient().get(keyByteSequence);
        GetResponse response = getFuture.get(requestTimeoutSeconds, TimeUnit.SECONDS);
        List<KeyValue> kvs = response.getKvs();
        if (kvs.size() > 1) {
            log.error("Multiple values found for key: " + key);
        }
        String value = null;
        for (KeyValue keyValue : kvs) {
            value = keyValue.getValue().toString(CHARSET);
            break;
        }
        return value;
	}

	@Override
	public String put(String key, String value) throws Exception {
		ByteSequence keyByteSequence = from(key);
        ByteSequence valueByteSequence = from(value);
        CompletableFuture<PutResponse> putResponseFuture = etcdConnection.getKvClient().put(keyByteSequence, valueByteSequence);
        return getPrevValue(putResponseFuture);
	}

	private String getPrevValue(CompletableFuture<PutResponse> putResponseFuture) throws InterruptedException, ExecutionException, TimeoutException {
		String prevValue = null;
        PutResponse putResponse = putResponseFuture.get(requestTimeoutSeconds, TimeUnit.SECONDS);
        KeyValue prevKv = putResponse.getPrevKv();
        if (prevKv != null) {
        	ByteSequence prevValueByteSequence = prevKv.getValue();
        	if (prevValueByteSequence != null) {
        		prevValue = prevValueByteSequence.toString();
        	}
        }
        return prevValue;
	}

	private ByteSequence from(String key) {
		return ByteSequence.from(key, CHARSET);
	}
	
	@Override
	public String put(String key, String value, long ttlSeconds) throws Exception {
		long leaseID = etcdConnection.getLeaseClient().grant(ttlSeconds).get().getID();
		keyToLeaseId.put(key, leaseID);
		ByteSequence keyByteSequence = from(key);
		ByteSequence valueByteSequence = from(key);
		CompletableFuture<PutResponse> putResponseFuture = etcdConnection.getKvClient().put(keyByteSequence, valueByteSequence, 
			PutOption.newBuilder().withLeaseId(leaseID).build());
		return getPrevValue(putResponseFuture);
	}

	@Override
	public void keepAlive(String key) throws Exception {
		Long leaseId = keyToLeaseId.get(key);
		
		// TODO change to DEBUG
		log.info("keepAlive leaseId: {}", leaseId);
		
		if (leaseId == null) { 
			throw new IOException("Key lease not found for key: " + key);
		}
		StreamObserver<LeaseKeepAliveResponse> observer = new StreamObserver<LeaseKeepAliveResponse>() {

			@Override
			public void onNext(LeaseKeepAliveResponse value) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onError(Throwable e) {
				log.error("Error onError: " + e.getClass() + ", " + e.getMessage(), e);
			}

			@Override
			public void onCompleted() {
				// TODO Auto-generated method stub
				
			}
		};
		CompletableFuture<LeaseKeepAliveResponse> LeaseKeepAliveResponseFuture = etcdConnection.getLeaseClient().keepAliveOnce(leaseId);
		LeaseKeepAliveResponse leaseKeepAliveResponse = LeaseKeepAliveResponseFuture.get(requestTimeoutSeconds, TimeUnit.SECONDS);
		if (leaseKeepAliveResponse == null) {
			throw new IOException("Keepalive did not succeed for key: " + key);
		}
//		leaseClient.keepAlive(leaseId, observer );
		
		// TODO remove from keyToLeaseId when expires
		
	}

	@Override
	public String remove(String key) throws Exception {
		ByteSequence keyByteSequence = from(key);
        CompletableFuture<DeleteResponse> deleteResponseFuture = etcdConnection.getKvClient().delete(keyByteSequence);
        DeleteResponse deleteResponse = deleteResponseFuture.get(requestTimeoutSeconds, TimeUnit.SECONDS);
        String prevValue = null;
        List<KeyValue> prevKv = deleteResponse.getPrevKvs();
        if (!prevKv.isEmpty()) {
        	if (prevKv.size() > 1) {
                log.error("Multiple values found when remove key: " + key);
            }
        	keyToLeaseId.remove(key);
        	ByteSequence prevValueByteSequence = prevKv.get(0).getValue();
        	if (prevValueByteSequence != null) {
        		prevValue = prevValueByteSequence.toString();
        	}
        }
        return prevValue;
	}

	@Override
	public void addListener(PersistencyListener persistencyListener, String prefix) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Collection<String> getAllKeys(String prefix) throws Exception {
		ByteSequence keysPrefixByteSequence = from(prefix);
        GetOption getOption = GetOption.newBuilder().isPrefix(true).build();
        CompletableFuture<GetResponse> getFuture = etcdConnection.getKvClient().get(keysPrefixByteSequence, getOption);
        GetResponse response = getFuture.get(requestTimeoutSeconds, TimeUnit.SECONDS);
        List<KeyValue> kvs = response.getKvs();
        List<String> keys = new ArrayList<>(response.getKvs().size());
        for (KeyValue keyValue : kvs) {
            String key = keyValue.getKey().toString(CHARSET);
            keys.add(key);
        }
        return keys;
	}

	@Override
	public Map<String, String> getAllKeyValues(String prefix) throws Exception {
		ByteSequence keysPrefixByteSequence = ByteSequence.from(prefix.getBytes());
        GetOption getOption = GetOption.newBuilder().isPrefix(true).build();
        CompletableFuture<GetResponse> getFuture = etcdConnection.getKvClient().get(keysPrefixByteSequence, getOption);
        GetResponse response = getFuture.get(requestTimeoutSeconds, TimeUnit.SECONDS);
        List<KeyValue> kvs = response.getKvs();
        Map<String, String> kvsMap = new HashMap<>(response.getKvs().size());
        for (KeyValue keyValue : kvs) {
            String key = keyValue.getKey().toString(CHARSET);
            kvsMap.put(key, keyValue.getValue().toString(CHARSET));
        }
        return kvsMap;
	}

	@Override
	public void deleteAllKVs(String prefix) throws Exception {
		log.info("Deleting all KVs for prefix: " + prefix);
        ByteSequence keysPrefixByteSequence = ByteSequence.from(prefix.getBytes());
        DeleteOption deleteOption = DeleteOption.newBuilder().isPrefix(true).build();
        CompletableFuture<DeleteResponse> deleteFuture = etcdConnection.getKvClient().delete(keysPrefixByteSequence, deleteOption);
        DeleteResponse response = deleteFuture.get(requestTimeoutSeconds, TimeUnit.SECONDS);
        List<KeyValue> kvs = response.getPrevKvs();
        log.info("Deleted kvs: {}", kvs);
	}
	
    public void close() {
        log.info("close");
//        shouldRun.set(false);
    }

}
