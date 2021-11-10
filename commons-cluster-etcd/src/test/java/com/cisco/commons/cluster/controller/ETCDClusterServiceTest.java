package com.cisco.commons.cluster.controller;

import java.util.Arrays;

import org.junit.Test;

import com.cisco.commons.cluster.controller.etcd.ETCDClusterService;
import com.cisco.commons.cluster.controller.etcd.ETCDConnection;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ETCDClusterServiceTest {

	@Test
	public void test() throws Exception {
		ETCDConnection etcdConnection = ETCDConnection.builder()
			.endpoints(Arrays.asList("http://127.0.0.1:2379")).connectTimeoutSeconds(2).build();
		etcdConnection.connect();

		ClusterService clusterService = null;
		if (etcdConnection.checkConnectivity()) {
			log.info("ETCD local connection found. Proceeding via ETCD service test.");
			clusterService = ETCDClusterService.builder().appId("testApp").instanceId("testInstance1")
				.etcdConnection(etcdConnection).build();
		} else {
			etcdConnection.close();
			clusterService = new MemoryClusterService();
			log.info("ETCD local connection not found. Proceeding via in-memory service test.");
		}

		clusterService.init();
		clusterService.put("k1", "v1");
		String v = clusterService.get("k1");
		System.out.println("zzz: " + v);
		clusterService.put("k2", "v2", 2);
		Thread.sleep(1000);
		v = clusterService.get("k2");
		System.out.println("zzz2: " + v);
		clusterService.keepAlive("k2");
		Thread.sleep(1000);
		clusterService.keepAlive("k2");
		Thread.sleep(1000);
		v = clusterService.get("k2");
		System.out.println("zzz3: " + v);
		Thread.sleep(3000);
		clusterService.keepAlive("k2");
		v = clusterService.get("k2");
		System.out.println("zzz4: " + v);
		etcdConnection.close();
		clusterService.close();
	}

}
