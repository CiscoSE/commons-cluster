package com.cisco.commons.cluster.controller.etcd;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import lombok.extern.slf4j.Slf4j;

/**
 * ETCD Utils.
 * 
 * @author Liran
 */
@Slf4j
public class ETCDUtils {
	
	private static final Charset CHARSET = StandardCharsets.UTF_8;
	
	private ETCDUtils() {
		
	}
	
	public static String get(String key, KV kvClient, long requestTimeoutSeconds) throws Exception {
		ByteSequence keyByteSequence = from(key);
        CompletableFuture<GetResponse> getFuture = kvClient.get(keyByteSequence);
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
	
	public static ByteSequence from(String key) {
		return ByteSequence.from(key, CHARSET);
	}
}
