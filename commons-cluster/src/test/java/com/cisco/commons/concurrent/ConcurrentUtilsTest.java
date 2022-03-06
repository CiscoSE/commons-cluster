package com.cisco.commons.concurrent;

import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * ConcurrentUtilsTest
 * 
 * @author Liran Mendelovich
 * 
 *         Copyright 2021 Cisco Systems Licensed under the Apache License,
 *         Version 2.0 (the "License"); you may not use this file except in
 *         compliance with the License. You may obtain a copy of the License at
 *         http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 *         applicable law or agreed to in writing, software distributed under
 *         the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *         CONDITIONS OF ANY KIND, either express or implied. See the License
 *         for the specific language governing permissions and limitations under
 *         the License.
 */
@Slf4j
public class ConcurrentUtilsTest {

	@Test
	public void waitForFinishTest() throws InterruptedException, ExecutionException {
		ExecutorService pool = Executors.newFixedThreadPool(8);
		List<Future<Boolean>> futures = new LinkedList<>();
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		futures.add(pool.submit(() -> runWithDelay(1)));
		futures.add(pool.submit(() -> runWithDelay(0)));
		futures.add(pool.submit(() -> runWithDelay(2)));
		boolean result = ConcurrentUtils.waitForFinish(futures , 3000);
		assertTrue(result);
		stopWatch.stop();
		assertTrue(stopWatch.getTime() >= 2000 && stopWatch.getTime() < 2800);
		if (pool instanceof ThreadPoolExecutor) {
			ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor)pool;
			
			// Stats are estimated approximate results, cannot rely on these.
			if (3 == threadPoolExecutor.getCompletedTaskCount()) {
				log.info("All tasks completed successfully.");
			} else {
				log.warn("Not all tasks may have been completed successfully.");
			}
		}
		ConcurrentUtils.shutdownAndAwaitTermination(pool, 1);
	}
	
	public boolean runWithDelay(int delaySeconds) {
		try {
			TimeUnit.SECONDS.sleep(delaySeconds);
			return true;
		} catch (Exception e) {
			log.error("Error in runWithDelay: " + e.getClass() + ", " + e.getMessage(), e);
			Assert.fail("Error in runWithDelay");
		}
		return false;
	}
	
}
