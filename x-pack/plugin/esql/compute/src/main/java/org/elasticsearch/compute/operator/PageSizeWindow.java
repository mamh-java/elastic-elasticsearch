/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

/**
 * A sliding-window buffer that maps sequential page IDs to their position counts.
 * <p>
 *     {@link MapExpandOperator} registers each source page via {@link #put}, and
 *     {@link MapContractOperator} retrieves and evicts entries via {@link #getAndEvict}.
 *     Because pages are processed in order, the window stays small (typically 1–2 entries).
 * </p>
 * <p>
 *     Memory is tracked against a {@link CircuitBreaker}.  The initial shallow size and
 *     initial (empty) array overhead are charged in the constructor; array growth is
 *     charged incrementally.  {@link #close()} releases all charged memory back to the
 *     breaker.
 * </p>
 * <p>
 *     The backing array is compacted on each {@link #getAndEvict} call by shifting
 *     elements left when the front is evicted.  Because the window is typically 1–2
 *     entries, these copies are essentially O(1).
 * </p>
 */
public class PageSizeWindow implements Accountable, Releasable {

    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(PageSizeWindow.class);

    private final CircuitBreaker breaker;
    /**
     * Backing array; {@code buf[0]} always corresponds to {@link #base}.
     */
    private int[] buf = new int[0];
    /**
     * The page ID at logical index 0 of the window (i.e. at {@code buf[0]}).
     * Starts at 1 because page generation is incremented before use.
     */
    private int base = 1;
    /**
     * Number of entries currently populated in {@link #buf} starting at index 0.
     */
    private int size = 0;

    /**
     * Creates a new window and immediately charges {@link #SHALLOW_SIZE} plus the initial
     * empty-array overhead to {@code breaker}.
     */
    public PageSizeWindow(CircuitBreaker breaker) {
        this.breaker = breaker;
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE + arrayBytes(0), "PageSizeWindow");
    }

    /**
     * Records the position count for a newly registered page ID.
     *
     * @throws IllegalArgumentException if {@code pageId < base} (already evicted)
     */
    public void put(int pageId, int posCount) {
        int slot = pageId - base;
        if (slot < 0) {
            throw new IllegalArgumentException("page id " + pageId + " already evicted (base=" + base + ")");
        }
        if (slot >= buf.length) {
            long oldArrayBytes = arrayBytes(buf.length);
            // Allocate exactly the needed capacity (slot+1).
            // The window is typically 1–2 entries, so no over-allocation heuristic is needed.
            int newLength = slot + 1;
            long newArrayBytes = arrayBytes(newLength);
            // Charge before allocating so an OOM break leaves memory state consistent.
            breaker.addEstimateBytesAndMaybeBreak(newArrayBytes - oldArrayBytes, "PageSizeWindow");
            int[] newBuf = new int[newLength];
            System.arraycopy(buf, 0, newBuf, 0, buf.length);
            buf = newBuf;
        }
        buf[slot] = posCount;
        // Update size to cover at least slot+1 entries.
        if (slot >= size) {
            size = slot + 1;
        }
    }

    /**
     * Returns the position count for {@code pageId} and evicts it from the window.
     * Compacts the window by shifting {@link #base} forward past any leading evicted slots.
     * <p>
     *     The backing array capacity is never reduced; the breaker charge remains until
     *     {@link #close()}.  Since the window is typically 1–2 entries the compaction
     *     copies are essentially O(1).
     * </p>
     *
     * @throws IllegalArgumentException if {@code pageId < base} (already evicted) or the
     *                                  slot was never registered
     */
    public int getAndEvict(int pageId) {
        int slot = pageId - base;
        if (slot < 0) {
            throw new IllegalArgumentException("page id " + pageId + " already evicted (base=" + base + ")");
        }
        if (slot >= size) {
            throw new IllegalArgumentException("page id " + pageId + " was never registered (base=" + base + ", size=" + size + ")");
        }
        int result = buf[slot];
        buf[slot] = 0;
        // Compact: shift the window left past any leading zeros.
        int leadingZeros = 0;
        while (leadingZeros < size && buf[leadingZeros] == 0) {
            leadingZeros++;
        }
        if (leadingZeros > 0) {
            int remaining = size - leadingZeros;
            if (remaining > 0) {
                System.arraycopy(buf, leadingZeros, buf, 0, remaining);
            }
            // Zero out the tail we vacated.
            for (int i = remaining; i < size; i++) {
                buf[i] = 0;
            }
            base += leadingZeros;
            size = remaining;
        }
        return result;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + arrayBytes(buf.length);
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-ramBytesUsed());
    }

    static long arrayBytes(int length) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) length * Integer.BYTES);
    }
}
