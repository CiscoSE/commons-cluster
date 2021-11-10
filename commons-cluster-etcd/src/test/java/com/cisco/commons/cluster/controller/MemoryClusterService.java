package com.cisco.commons.cluster.controller;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import net.jodah.expiringmap.ExpirationListener;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

/**
 * 
 * In-memory implementation.
 * Intended for testing purposes only.
 * Key prefixes operations are not thread-safe.
 * Listeners events are synchronous blocking calls and expected to be fast.
 * 
 * @author Liran
 *
 */
public class MemoryClusterService implements ClusterService {

	private static final String KEY_SEPARATOR = "/";
	
	private ExpiringMap<String, String> map;
	
	private Map<String, PersistencyListener> persistencyListeners;

	private Multimap<String, String> keyPrefixToKeys;
	
	@Override
	public void init() {
		map = ExpiringMap.builder()
		.variableExpiration()
		.build();
		
		persistencyListeners = new ConcurrentHashMap<>();
		keyPrefixToKeys = Multimaps.synchronizedMultimap(HashMultimap.create());
		
		ExpirationListener<String, String> expirationListener = (key, value) -> {triggerEvent(key, value, null, EventType.DELETED);};
		map.addExpirationListener(expirationListener);
	}
	
	@Override
	public String get(String key) throws IOException {
		return map.get(key);
	}

	@Override
	public String put(String key, String value) throws IOException {
		String prefix = getPrefix(key);
		if (prefix != null) {
			keyPrefixToKeys.put(prefix, key);
		}
		String oldValue = map.put(key, value);
		EventType eventType = oldValue == null ? EventType.CREATED : EventType.UPDATED;
		triggerEvent(key, oldValue, value, eventType);
		return oldValue;
	}

	private String getPrefix(String key) {
		int prefixLastIndex = key.lastIndexOf(KEY_SEPARATOR);
		String prefix = null;
		if (prefixLastIndex >= 0) {
			prefix = key.substring(0, prefixLastIndex);
		}
		return prefix;
	}
	
	@Override
	public String put(String key, String value, long ttlSeconds) throws IOException {
		String oldValue = map.put(key, value, ExpirationPolicy.CREATED, ttlSeconds, TimeUnit.SECONDS);
		triggerEvent(key, oldValue, value, EventType.CREATED);
		return oldValue;
	}
	
	private void triggerEvent(String key, String oldValue, String newValue, EventType eventType) {
		Set<Entry<String, PersistencyListener>> persistencyListenersEntries = persistencyListeners.entrySet();
		for (Entry<String, PersistencyListener> persistencyListenerEntry : persistencyListenersEntries) {
			if (key.startsWith(persistencyListenerEntry.getKey())) {
				persistencyListenerEntry.getValue().event(key, oldValue, newValue, eventType);
			}
		}
	}

	@Override
	public void keepAlive(String key) throws IOException {
		map.resetExpiration(key);
	}

	@Override
	public String remove(String key) throws IOException {
		String oldValue = map.remove(key);
		String prefix = getPrefix(key);
		if (prefix != null) {
			keyPrefixToKeys.remove(prefix, key);
			if (keyPrefixToKeys.get(prefix).isEmpty()) {
				keyPrefixToKeys.removeAll(prefix);
			}
		}
		triggerEvent(key, oldValue, null, EventType.DELETED);
		return oldValue;
	}
	
	@Override
	public void addListener(PersistencyListener persistencyListener, String prefix) {
		persistencyListeners.put(prefix, persistencyListener);
	}

	@Override
	public Collection<String> getAllKeys(String prefix) throws IOException {
		return keyPrefixToKeys.get(prefix);
	}

	@Override
	public Map<String, String> getAllKeyValues(String prefix) throws IOException {
		Map<String, String> kvs = new HashMap<>();
		Collection<String> keys = keyPrefixToKeys.get(prefix);
		for (String key: keys) {
			kvs.put(key, map.get(key));
		}
		return kvs;
	}

	@Override
	public void deleteAllKVs(String prefix) throws Exception {
		Collection<String> keys = keyPrefixToKeys.get(prefix);
		for (String key: keys) {
			map.remove(key);
		}
	}
	
	@Override
	public void close() {
		
	}

}
