package com.cisco.commons.initializer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.concurrent.CircuitBreaker;

import com.cisco.commons.concurrent.ConcurrentUtils;
import com.google.common.base.Preconditions;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

/**
 * # Initializer for service initialization logic.
 * 
 * This can be very useful for distributed services, to meet several requirements:
 * * Start the service first and report healthy state
 * * Initialize mandatory components (database, messaging service)
 * 	* in a background task for not holding service startup.
 * 	* Retry mechanism – per component, not for all components together.
 *  * Circuit breaker for retrying for waiting for some cluster state which can hold service initialization.
 *  * Optional non-mandatory comonents can be retried and initialized in a background task without holding
 *    application from starting and serving requests and/or do its work.
 * 	* Serve requests and/or do the work only after all mandatory components initialization tasks are done.
 * 
 * During initialization, application service can check health of the components and reflect it accordingly.
 * 
 * Example usage:
 * ```
 * Initializer initializer = Initializer.builder().circuitBreaker(circuitBreaker)
 *	.mandatoryTask("initDB", () -> {return initDB();})
 *	.mandatoryTask("initMessagingService", () -> {return initMessagingService();})
 *	.nonMandatoryTask("initStats", () -> {return initStats();})
 *	.postInitTask("serveRequests", () -> {return serveRequests();})
 *	.build();
 * initializer.initAsync();
 * ```
 * 
 * @author Liran Mendelovich
 * 
 * Copyright 2021 Cisco Systems Licensed under the Apache License,
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
public class Initializer {

	public enum InitializerStatus {
		NOT_STARTED, INITIALIZING, DONE_NOT_FINISHED, SUCCESS, CLOSED
    }
	
	private CircuitBreaker<?> circuitBreaker;
	
	/*
	 *  Pending, adding backoff retry policy - pending enhancement, see:
	 *  https://github.com/google/guava/issues/5930
	 */
	private Integer failureDelaySeconds = 30;
	
	private Integer postInitGracePeriodSeconds;
	private ExecutorService initTasksPool;
	private ExecutorService mainTaskPool;
	private Map<String, Callable<Boolean>> mandatoryTasks = new LinkedHashMap<>();
	private List<Future<Boolean>> mandatoryTasksFutures = new LinkedList<>();
	private Map<String, Callable<Boolean>> nonMandatoryTasks = new LinkedHashMap<>();
	private List<Future<Boolean>> nonMandatoryTasksFutures = new LinkedList<>();
	private Map<String, Callable<Boolean>> postInitTasks = new LinkedHashMap<>();
	private List<Future<Boolean>> postInitTasksFutures = new LinkedList<>();
	@Getter private int successfulTasks = 0;
	private InitializerStatus initializerStatus = InitializerStatus.NOT_STARTED;
	private AtomicBoolean isInitialized = new AtomicBoolean(false);
	private AtomicBoolean shouldRun = new AtomicBoolean(true);
	
	@Builder
	public Initializer(CircuitBreaker<?> circuitBreaker, Integer postInitGracePeriodSeconds, 
			ExecutorService initTasksPool, @Singular Map<String, Callable<Boolean>> mandatoryTasks, 
			@Singular Map<String, Callable<Boolean>> nonMandatoryTasks, @Singular Map<String, Callable<Boolean>> postInitTasks,
			Integer failureDelaySeconds) {
		super();
		log.info("Initializer building begin");
		this.circuitBreaker = circuitBreaker;
		if (failureDelaySeconds != null) {
			Preconditions.checkArgument(failureDelaySeconds > 0, "failureDelaySeconds need to be positive");
			this.failureDelaySeconds = failureDelaySeconds;
		}
		if (postInitGracePeriodSeconds != null) {
			Preconditions.checkArgument(postInitGracePeriodSeconds > 0, "postInitGracePeriodSeconds need to be positive");
			this.postInitGracePeriodSeconds = postInitGracePeriodSeconds;
		}
		this.initTasksPool = initTasksPool;
		if (this.initTasksPool == null) {
			this.initTasksPool = Executors.newCachedThreadPool();
		}
		mainTaskPool = Executors.newSingleThreadExecutor();
		this.mandatoryTasks = mandatoryTasks;
		this.nonMandatoryTasks = nonMandatoryTasks;
		this.postInitTasks = postInitTasks;
		mandatoryTasksFutures = new ArrayList<>(mandatoryTasks.size());
		nonMandatoryTasksFutures = new ArrayList<>(nonMandatoryTasks.size());
		postInitTasksFutures = new ArrayList<>(postInitTasks.size());
		log.info("Initializer building done");
	}
	
	public void initAsync() {
		log.info("initAsync begin");
		Preconditions.checkArgument(isInitialized.compareAndSet(false, true), "already initialized");
		initializerStatus = InitializerStatus.INITIALIZING;
		List<Callable<Boolean>> mandatoryInitTasks = new ArrayList<>(mandatoryTasks.size());
		Set<Entry<String, Callable<Boolean>>> mandatoryTasksEntries = mandatoryTasks.entrySet();
		for (Entry<String, Callable<Boolean>> mandatoryTaskEntry : mandatoryTasksEntries) {
			Callable<Boolean> initTask = () -> {
				return executeTaskUntilSucceed(mandatoryTaskEntry.getValue(), mandatoryTaskEntry.getKey());
	        };
	        mandatoryInitTasks.add(initTask);
		}
		Callable<Boolean> mandatoryAndPostInitTasksTask = () -> {
			try {
				executeMandatoryAndPostInitTasksTask(mandatoryInitTasks);
				return true;
			} catch (Exception e) {
				log.debug("Error on mandatoryAndPostInitTasksTask: " + e.getClass() + ", " + e.getMessage(), e);
				log.info("Error on mandatoryAndPostInitTasksTask: " + e.getClass() + ", " + e.getMessage());
				return false;
			}
        };
        mainTaskPool.submit(mandatoryAndPostInitTasksTask);
		log.info("initAsync end");
	}

	private void executeMandatoryAndPostInitTasksTask(List<Callable<Boolean>> mandatoryInitTasks) throws InterruptedException, ExecutionException {
		log.info("Invoking all mandatory init tasks.");
		mandatoryTasksFutures = initTasksPool.invokeAll(mandatoryInitTasks);
		log.info("Invoked all mandatory init tasks.");
		Set<Entry<String, Callable<Boolean>>> nonMandatoryTasksEntries = nonMandatoryTasks.entrySet();
		List<Callable<Boolean>> nonMandatoryInitTasks = new ArrayList<>(nonMandatoryTasks.size());
		for (Entry<String, Callable<Boolean>> nonMandatoryTaskEntry : nonMandatoryTasksEntries) {
			Callable<Boolean> initTask = () -> {
				return executeTaskUntilSucceed(nonMandatoryTaskEntry.getValue(), nonMandatoryTaskEntry.getKey());
		    };
		    nonMandatoryInitTasks.add(initTask);
		}
		log.info("Invoking non mandatory init tasks.");
		for (Callable<Boolean> nonMandatoryInitTask: nonMandatoryInitTasks) {
			Future<Boolean> future = initTasksPool.submit(nonMandatoryInitTask);
			nonMandatoryTasksFutures.add(future);
		}
		if (postInitGracePeriodSeconds != null && postInitGracePeriodSeconds > 0 && 
				!nonMandatoryTasks.isEmpty()) {
			log.info("Delaying for grace period of {} seconds before proceeding to post init tasks.", postInitGracePeriodSeconds);
			long postInitGracePeriodMillis = postInitGracePeriodSeconds * 1000l;
			boolean allSucceed = ConcurrentUtils.waitForFinish(nonMandatoryTasksFutures, postInitGracePeriodMillis);
			log.info("Done waiting for non mandatory init tasks for grace period, proceeding to post init tasks. allSucceed: {}", allSucceed);
			log.info("Invoked non mandatory init tasks.");
		}
		log.info("Invoking post init tasks.");
		Set<Entry<String, Callable<Boolean>>> postInitTasksEntries = postInitTasks.entrySet();
		for (Entry<String, Callable<Boolean>> postInitTaskEntry : postInitTasksEntries) {
			Callable<Boolean> initTask = () -> {
				return executeTaskUntilSucceed(postInitTaskEntry.getValue(), postInitTaskEntry.getKey());
		    };
		    Future<Boolean> future = initTasksPool.submit(initTask);
		    postInitTasksFutures.add(future);
		}
		log.info("Invoked post init tasks.");
		waitForResults();
	}

	private void waitForResults() throws InterruptedException, ExecutionException {
		boolean isAllDone = true;
		for (Future<Boolean> mandatoryTasksFuture: mandatoryTasksFutures) {
			Boolean result = mandatoryTasksFuture.get();
			if (result.booleanValue()) {
				successfulTasks++;
			} else {
				isAllDone = false;
			}
			isAllDone = isAllDone && result;
		}
		for (Future<Boolean> nonMandatoryTasksFuture: nonMandatoryTasksFutures) {
			Boolean result = nonMandatoryTasksFuture.get();
			if (result.booleanValue()) {
				successfulTasks++;
			} else {
				isAllDone = false;
			}
		}
		for (Future<Boolean> postInitTasksFuture: postInitTasksFutures) {
			Boolean result = postInitTasksFuture.get();
			if (result.booleanValue()) {
				successfulTasks++;
			} else {
				isAllDone = false;
			}
		}
		if (isAllDone && shouldRun.get()) {
			initializerStatus = InitializerStatus.SUCCESS;
			log.info("Done waiting for all tasks with success.");
		} else {
			initializerStatus = InitializerStatus.DONE_NOT_FINISHED;
			log.info("Done waiting for all tasks, not all tasks are done.");
		}
	}
	
	private boolean executeTaskUntilSucceed(Callable<Boolean> action, String actionLabel) {
    	log.info("Executing action - begin: {}", actionLabel);
    	boolean success = false;
    	while (!success && shouldRun.get()) {
    		boolean considerCircuitBreaker = true;
    		if (action instanceof Task) {
    			considerCircuitBreaker = ((Task)action).isConsiderCircuitBreaker();
    		}
    		boolean holdAction = considerCircuitBreaker && circuitBreaker != null && circuitBreaker.isOpen();
    		if (holdAction) {
    			log.info("circuit breaker is open. Holding action: {}", actionLabel);
    		} else {
    			success = executeAction(action, actionLabel);
    		}
    		if (!success && shouldRun.get()) {
                log.info("Delaying {} seconds before retrying action: {}", failureDelaySeconds, actionLabel);
                delay(failureDelaySeconds);
    		}
    	}
    	log.info("Executing action - end: {}", actionLabel);
    	return success;
    }

	private boolean executeAction(Callable<Boolean> action, String actionLabel) {
		boolean success = false;
		try {
			log.info("Executing action - attempt: {}", actionLabel);
		    Boolean result = action.call();
		    if (result.booleanValue()) {
		    	success = true;
		    } else {
		    	log.error("{} result is false.", actionLabel);
		    }
		} catch (Exception e) {
		    log.error("Error executing action: " + actionLabel + ": " + e.getClass() + ", " + e.getMessage(), e);
		}
		return success;
	}
	
	public InitializerStatus getStatus() {
		return initializerStatus;
	}
	
	private void delay(long seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (Exception e) {
            log.info("Stopped delaying", e);
        }
    }
	
	public void shutdown() {
		log.info("shutdown begin");
		shouldRun.set(false);
		ConcurrentUtils.shutdownAndAwaitTermination(initTasksPool, 40);
		ConcurrentUtils.shutdownAndAwaitTermination(mainTaskPool, 40);
		initializerStatus = InitializerStatus.CLOSED;
		log.info("shutdown end");
	}

}
