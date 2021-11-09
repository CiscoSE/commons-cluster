package com.cisco.commons.cluster.controller;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

public class ExpiringMapTemp {
	
	public static void main(String[] args) throws InterruptedException, IOException {
		ExpiringMap<String, String> map = ExpiringMap.builder()
			.variableExpiration()
			.build();
		
		MemoryClusterService memoryPersistency = new MemoryClusterService();
		memoryPersistency.init();
		memoryPersistency.put("aa/bb/cc1", "v1");
		memoryPersistency.put("aa/bb/cc2", "v2");
		System.out.println("zzz " + memoryPersistency.getAllKeys("aa/bb"));

		map.put("k1", "v1", ExpirationPolicy.CREATED, 5, TimeUnit.SECONDS);
		String v = map.get("k1");
		System.out.println("zzz1 " + v);
		Thread.sleep(3000);
		v = map.get("k1");
		System.out.println("zzz2 " + v);
//		map.put("k1", "v1", ExpirationPolicy.CREATED, 5, TimeUnit.SECONDS);
		map.resetExpiration("k1");
		v = map.get("k1");
		System.out.println("zzz3 " + v);
		Thread.sleep(3000);
		v = map.get("k1");
		System.out.println("zzz4 " + v);
		Thread.sleep(6000);
		v = map.get("k1");
		System.out.println("zzz5 " + v);
	}
}
