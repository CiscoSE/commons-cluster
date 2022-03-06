package com.cisco.commons.concurrent;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.time.StopWatch;

import lombok.extern.slf4j.Slf4j;

/**
 * Concurrent / Concurrency utilities.
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
public class ConcurrentUtils {

	private ConcurrentUtils() {

	}

	/**
	 * Graceful shutdown a thread pool.
	 * {@link https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ExecutorService.html}
	 * @param pool - thread pool
	 * @param timeoutSeconds - grace period timeout in seconds - timeout can be twice than this value, as first it
	 * waits for existing tasks to terminate, then waits for cancelled tasks to terminate.
	 */
	public static void shutdownAndAwaitTermination(ExecutorService pool, int timeoutSeconds) {
		
		// Disable new tasks from being submitted
		pool.shutdown();
		try {
			
			// Wait a while for existing tasks to terminate
			if (!pool.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
				
				// Cancel currently executing tasks - best effort, based on interrupt handling implementation.
				pool.shutdownNow();
				
				// Wait a while for tasks to respond to being cancelled
				if (!pool.awaitTermination(timeoutSeconds, TimeUnit.SECONDS))
					log.error("Thread pool did not shutdown all tasks after the timeout: {} seconds.", timeoutSeconds);
			}
		} catch (InterruptedException e) {
			
			log.info("Current thread interrupted during shutdownAndAwaitTermination, calling shutdownNow.");
			
			// (Re-)Cancel if current thread also interrupted
			pool.shutdownNow();
			
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}
	
	public static Map<String, String> calculatePoolStats(ThreadPoolExecutor threadPoolExecutor, String label) {
		Map<String, String> stats = new LinkedHashMap<>();
		stats.put(label + ".pool.active.count", String.valueOf(threadPoolExecutor.getActiveCount()));
		stats.put(label + ".pool.completed.tasks.count", String.valueOf(threadPoolExecutor.getCompletedTaskCount()));
		stats.put(label + ".pool.core.pool.size", String.valueOf(threadPoolExecutor.getCorePoolSize()));
		stats.put(label + ".pool.largest.pool.size", String.valueOf(threadPoolExecutor.getLargestPoolSize()));
		stats.put(label + ".pool.max.pool.size", String.valueOf(threadPoolExecutor.getMaximumPoolSize()));
		stats.put(label + ".pool.pool.size", String.valueOf(threadPoolExecutor.getPoolSize()));
		stats.put(label + ".pool.tasks.count", String.valueOf(threadPoolExecutor.getTaskCount()));
		stats.put(label + ".pool.queue.size", String.valueOf(threadPoolExecutor.getQueue().size()));
		return Collections.unmodifiableMap(stats);
	}
	
	/**
	 * Wait for all tasks to finish up to the timeout.
	 * @param futures tasks
	 * @param timeoutMillis timeout in milliseconds
	 * @return true if all succeed in time, false otherwise.
	 * @throws InterruptedException if interrupted
	 * @throws ExecutionException if got execution exception
	 */
	public static boolean waitForFinish(List<Future<Boolean>> futures, long timeoutMillis) throws InterruptedException, ExecutionException {
		boolean isTimeoutReached = false;
		Iterator<Future<Boolean>> nonMandatoryTasksFuturesIterator = futures.iterator();
		StopWatch stopWatch = new StopWatch("waitForFinish");
		stopWatch.start();
		boolean allSucceed = true;
		while (!isTimeoutReached && nonMandatoryTasksFuturesIterator.hasNext()) {
			Future<Boolean> nonMandatoryTasksFuture = nonMandatoryTasksFuturesIterator.next();
			try {
				long elapsedTime = stopWatch.getTime();
				if (elapsedTime > timeoutMillis) {
					log.info("elapsedTime is more than grace period");
					isTimeoutReached = true;
					allSucceed = false;
					break;
				}
				boolean result = nonMandatoryTasksFuture.get(timeoutMillis - elapsedTime, TimeUnit.MILLISECONDS);
				if (!result) {
					allSucceed = false;
				}
			} catch (TimeoutException e) {
				isTimeoutReached = true;
				allSucceed = false;
			}
		}
		stopWatch.stop();
		log.info("waitForFinish done. allSucceed: {}, isTimeoutReached: {}, elapsedTime: {}", allSucceed, isTimeoutReached, stopWatch.getTime());
		return allSucceed;
	}
}
