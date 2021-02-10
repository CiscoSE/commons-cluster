package com.cisco.commons.cluster.controller;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventSchedulerTest {

    @Test
    public void eventSchedulerTest() throws InterruptedException {
        log.info("EventScheduler test start");
        AtomicInteger stateChangedCount = new AtomicInteger();
        ClusterEventListener eventListener = new ClusterEventListener() {

            @Override
            public void takeLeadership() {
                log.debug("takeLeadership");
            }

            @Override
            public void stateChanged() {
                log.info("state changed");
                stateChangedCount.incrementAndGet();
            }

            @Override
            public void onMessage(String message) {
                log.debug("onMessage");
            }
        };
        ClusterEventScheduler clusterEventScheduler = new ClusterEventScheduler(eventListener);
        clusterEventScheduler.setDelayTime(1, TimeUnit.SECONDS);
        clusterEventScheduler.scheduleEvent();
        Thread.sleep(100);
        clusterEventScheduler.scheduleEvent();

        Thread.sleep(1500);

        /*
         * We expect events:
         * - single stateChanged event (instead of 2) for both calls of scheduleEvent() in less than 1 second configured delay time.
         */
        assertEquals(1, stateChangedCount.intValue());
    }

}
