package com.cisco.commons.cluster.controller;

import java.util.Arrays;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ETCDClusterServiceTest {

//	@Test
	public void test() {
		
	}
	
	public static void main(String[] args) throws Exception {
		ETCDClusterService etcdClusterService = ETCDClusterService.builder().appId("testApp").connectTimeoutSeconds(5)
			.endpoints(Arrays.asList("http://127.0.0.1:2379")).instanceId("testInstance1").requestTimeoutSeconds(3).build();
		etcdClusterService.init(null);
		etcdClusterService.put("k1", "v1");
		String v = etcdClusterService.get("k1");
		System.out.println("zzz: " + v);
		etcdClusterService.put("k2", "v2", 2);
		Thread.sleep(1000);
		v = etcdClusterService.get("k2");
		System.out.println("zzz2: " + v);
		etcdClusterService.keepAlive("k2");
		Thread.sleep(1000);
		etcdClusterService.keepAlive("k2");
		Thread.sleep(1000);
		v = etcdClusterService.get("k2");
		System.out.println("zzz3: " + v);
		Thread.sleep(3000);
		etcdClusterService.keepAlive("k2");
		v = etcdClusterService.get("k2");
		System.out.println("zzz4: " + v);
		etcdClusterService.close();
	}
	
}
