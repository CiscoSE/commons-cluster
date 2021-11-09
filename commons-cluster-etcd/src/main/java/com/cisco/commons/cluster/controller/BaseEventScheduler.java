package com.cisco.commons.cluster.controller;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

/**
 * Solution for optimized coping with multiple events on a time window.
 * 
 * @author etroth
 * 
 * Copyright 2021 Cisco Systems
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@Slf4j
public abstract class BaseEventScheduler {

    protected static int DELAY_TIME = 5;
    protected static TimeUnit DELAY_TIME_UNIT = TimeUnit.MINUTES;

    protected ScheduledThreadPoolExecutor eventProcessingPool;

    public BaseEventScheduler() {
        this.eventProcessingPool = new ScheduledThreadPoolExecutor(1);
    }

    protected void executeEventProcessing() throws InterruptedException {
        log.debug("executeEventProcessing");
        if (eventProcessingPool.getActiveCount() > 0 || !eventProcessingPool.getQueue().isEmpty()) {
            log.debug("Ignoring event, as event is already scheduled");
            return;
        }
        Runnable eventProcessorTask = () -> {
            processEvent();
        };
        log.debug("Scheduling event processing");
        eventProcessingPool.schedule(eventProcessorTask, DELAY_TIME, DELAY_TIME_UNIT);
    }

    protected void setDelayTime(int delayTime, TimeUnit timeUnit) {
        DELAY_TIME = delayTime;
        DELAY_TIME_UNIT = timeUnit;
    }

    protected abstract void processEvent();

    public void shutdown() {
        try {
            log.info("shutdown: " + getClass().getSimpleName());
            eventProcessingPool.shutdown();
        } catch (Exception e) {
            log.error("Error shutdown");
        }
    }
}
