package com.cisco.commons.cluster.controller;

import java.util.Arrays;

import com.cisco.commons.cluster.controller.etcd.ETCDClusterService;
import com.cisco.commons.cluster.controller.etcd.ETCDConnection;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ETCDClusterServiceTest {

//	@Test
	public void test() {
		
	}
	
	public static void main(String[] args) throws Exception {
		
		ETCDConnection etcdConnection = ETCDConnection.builder()
			.endpoints(Arrays.asList("http://127.0.0.1:2379")).build();
		etcdConnection.connect();
		
		ETCDClusterService etcdClusterService = ETCDClusterService.builder().appId("testApp").instanceId("testInstance1")
			.requestTimeoutSeconds(3).etcdConnection(etcdConnection).build();
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
		etcdConnection.close();
		etcdClusterService.close();
	}
	
}
