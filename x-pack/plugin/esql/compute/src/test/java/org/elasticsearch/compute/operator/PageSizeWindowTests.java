/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class PageSizeWindowTests extends ESTestCase {

    public void testBasicPutAndGet() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(10_000));
        try (PageSizeWindow window = new PageSizeWindow(breaker)) {
            window.put(1, 10);
            window.put(2, 20);
            window.put(3, 30);
            assertThat(window.getAndEvict(1), equalTo(10));
            assertThat(window.getAndEvict(2), equalTo(20));
            assertThat(window.getAndEvict(3), equalTo(30));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testEvictionAdvancesBase() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(10_000));
        try (PageSizeWindow window = new PageSizeWindow(breaker)) {
            window.put(1, 42);
            window.getAndEvict(1);
            // After evicting page 1, base should have advanced past it.
            expectThrows(IllegalArgumentException.class, () -> window.getAndEvict(1));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testWindowStaysSmall() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(10_000));
        long maxAllowed = PageSizeWindow.SHALLOW_SIZE + PageSizeWindow.arrayBytes(2);
        try (PageSizeWindow window = new PageSizeWindow(breaker)) {
            for (int i = 1; i <= 1000; i++) {
                window.put(i, i * 7);
                window.getAndEvict(i);
                assertThat(window.ramBytesUsed() + " exceeds limit at page " + i, window.ramBytesUsed() <= maxAllowed, equalTo(true));
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBreakOnGrow() {
        // Allow exactly what the constructor charges (SHALLOW_SIZE + arrayBytes(0)) but
        // not the additional growth when put() needs to extend the array.
        long limit = PageSizeWindow.SHALLOW_SIZE + PageSizeWindow.arrayBytes(0);
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(limit));
        try (PageSizeWindow window = new PageSizeWindow(breaker)) {
            expectThrows(CircuitBreakingException.class, () -> window.put(1, 10));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBreakerReleasedOnClose() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(10_000));
        PageSizeWindow window = new PageSizeWindow(breaker);
        assertThat(breaker.getUsed(), greaterThan(0L));
        window.put(1, 100);
        window.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testRamBytesUsedMatchesActual() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(10_000));
        try (PageSizeWindow window = new PageSizeWindow(breaker)) {
            window.put(1, 10);
            window.put(2, 20);
            long reported = window.ramBytesUsed();
            // RamUsageTester measures the actual object graph; exclude breaker (shared).
            long actual = RamUsageTester.ramUsed(window) - RamUsageTester.ramUsed(breaker);
            assertThat(reported, equalTo(actual));
            assertThat(reported, equalTo(breaker.getUsed()));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testOutOfOrderGetThrows() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(10_000));
        try (PageSizeWindow window = new PageSizeWindow(breaker)) {
            // Page 5 was never registered.
            expectThrows(IllegalArgumentException.class, () -> window.getAndEvict(5));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testGapInPageIds() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(10_000));
        try (PageSizeWindow window = new PageSizeWindow(breaker)) {
            window.put(1, 11);
            window.put(3, 33); // skip 2
            assertThat(window.getAndEvict(1), equalTo(11));
            assertThat(window.getAndEvict(3), equalTo(33));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Helper matching {@link BreakingBytesRefBuilderTests} to compute array RAM.
     */
    private long arrayBytes(int length) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) length * Integer.BYTES);
    }
}
