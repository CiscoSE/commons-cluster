package com.cisco.commons.initializer;

import java.util.concurrent.Callable;

import lombok.Builder;
import lombok.Getter;

/**
 * Task wrapper.
 * 
 * considerCircuitBreaker - Whether to consider circuit breaker when executing this task. Default value is true.
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
@Getter
public class Task implements Callable<Boolean> {

	private Callable<Boolean> action;
	private boolean considerCircuitBreaker = true;
	
	@Builder
	public Task(Callable<Boolean> action, boolean considerCircuitBreaker) {
		super();
		this.action = action;
		this.considerCircuitBreaker = considerCircuitBreaker;
	}

	@Override
	public Boolean call() throws Exception {
		return action.call();
	}
}
