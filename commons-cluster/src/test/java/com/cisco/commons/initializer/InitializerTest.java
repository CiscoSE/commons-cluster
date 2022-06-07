package com.cisco.commons.initializer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.concurrent.CircuitBreaker;
import org.junit.Test;

import com.cisco.commons.initializer.Initializer.InitializerStatus;

import lombok.extern.slf4j.Slf4j;

/**
 * Initializer test.
 * 
 * @author Liran Mendelovich
 * 
 *  Copyright 2021 Cisco Systems Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under
 * the License.
 */
@Slf4j
public class InitializerTest {

	private AtomicBoolean isDBInitialized = new AtomicBoolean(false);
	private AtomicBoolean isInitStatsInitialized = new AtomicBoolean(false);
	private AtomicBoolean isCircuitBreakerInitialized = new AtomicBoolean(false);
	
	@Test
	public void initializerTest() throws InterruptedException {
		CircuitBreaker<?> circuitBreaker = new CircuitBreaker<Boolean>() {

			@Override
			public boolean isOpen() {
				
				// Get actual external value on application
//				boolean isMaintenanceModeEnabled = false;
//				
//				return isMaintenanceModeEnabled;
				
				if (isCircuitBreakerInitialized.compareAndSet(false, true)) {
					log.info("Simulating CircuitBreaker open");
					return true;
				}
				return false;
				
			}

			@Override
			public boolean isClosed() {
				return true;
			}

			@Override
			public boolean checkState() {
				return isClosed();
			}

			@Override
			public void close() {
				// do nothing
			}

			@Override
			public void open() {
				// do nothing
			}

			@Override
			public boolean incrementAndCheckState(Boolean increment) {
				return isClosed();
			}
			
		};
		Task preInitTask = Task.builder()
				.action(() -> {return preInit();})
				.considerCircuitBreaker(false)
				.build();
		Initializer initializer = Initializer.builder().circuitBreaker(circuitBreaker)
			.mandatoryTask("preInit", preInitTask)
			.mandatoryTask("initDB", () -> {return initDB();})
			.mandatoryTask("initMessagingService", () -> {return initMessagingService();})
			.nonMandatoryTask("initStats", () -> {return initStats();})
			.postInitTask("serveRequests", () -> {return serveRequests();})
			.failureDelaySeconds(1)
			.postInitGracePeriodSeconds(1)
			.build();
		assertEquals(InitializerStatus.NOT_STARTED, initializer.getStatus());
		initializer.initAsync();
		log.info("Invoked initAsync, pending for results.");
		TimeUnit.SECONDS.sleep(4);
		assertEquals(InitializerStatus.SUCCESS, initializer.getStatus());
		initializer.shutdown();
		assertEquals(InitializerStatus.CLOSED, initializer.getStatus());
		assertEquals(5, initializer.getSuccessfulTasks());
	}
	
	private Boolean preInit() {
		return true;
	}

	private boolean initMessagingService() {
		return true;
	}

	private boolean initDB() {
		if (isDBInitialized.compareAndSet(false, true)) {
			log.info("Simulating DB down, failing initDB");
			return false;
		}
		return true;
	}
	
	private boolean initStats() {
		if (isInitStatsInitialized.compareAndSet(false, true)) {
			log.info("Simulating stats down, failing initStats");
			return false;
		}
		return true;
	}
	
	private boolean serveRequests() {
		return true;
	}
	
}
