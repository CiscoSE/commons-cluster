package com.cisco.commons.cluster.controller;

import lombok.extern.slf4j.Slf4j;

/**
 * Solution for optimized coping with multiple events on a time window.
 * Cluster Controller Leader can get multiple notifications of cacheChanged event in a time window (when other followers were added/removed),
 * such as initial cluster startup where many service instances can start together
 * in a short time window.
 * For example, for an app's LEADER instance: 
 * followerA join - stateChanged event, 
 * and 1 min afterwards, 
 * followerB join - stateChanged event
 * This is to avoid start processing of the callback method (which may be a heavy/long operation),
 * and let other stateChanged events arrive during the time window.
 * A new operation will get ignored when we already have a current one that should be processed soon.
 * Then we will process only one event per added/removed followers.
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
 *
 */
@Slf4j
public class ClusterEventScheduler extends BaseEventScheduler {

    private ClusterEventListener eventListener;

    public ClusterEventScheduler(ClusterEventListener eventListener) {
        super();
        this.eventListener = eventListener;
    }

    protected void setEventListener(ClusterEventListener eventListener) {
        this.eventListener = eventListener;
    }

    public void scheduleEvent() throws InterruptedException {
        log.debug("Scheduling cluster event.");
        executeEventProcessing();
    }

    @Override
    protected void processEvent() {
        log.info("processEvent - triggering stateChanged");
        eventListener.stateChanged();
    }

}
