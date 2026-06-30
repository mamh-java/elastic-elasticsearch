/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;

/**
 * Tracks the source {@link Page} currently being processed by a MAP expand/contract
 * operator pair.
 * <p>
 *     {@link MapExpandOperator} calls {@link #onPageStart} at the beginning of each
 *     source page and {@link #onPageComplete} after emitting all expanded rows for that
 *     page.  {@link MapContractOperator} uses {@link #currentPageSize()} to know how many
 *     positions the current source page has, and {@link #currentPageGeneration()} to
 *     detect page boundaries when {@code _map_pos} alone is ambiguous (e.g. when source
 *     pages contain a single document each and {@code pos} always resets to {@code 0}).
 * </p>
 * <p>
 *     Both operators share the same {@code MapPageTracker} instance, which is created
 *     once in the enclosing {@link Operator.OperatorFactory#get(DriverContext)} call
 *     and passed to both constructors.
 * </p>
 */
public final class MapPageTracker extends DriverLocalChannel {

    private Page currentPage;
    private int positionCount;
    private boolean active;
    /**
     * Monotonically increasing counter incremented on every {@link #onPageStart} call.
     * {@link MapContractOperator} uses this to detect page boundaries when
     * {@code _map_pos} would otherwise be ambiguous.
     */
    private long pageGeneration;
    /**
     * The position count of the most recently started source page.  Unlike
     * {@link #positionCount}, this value is NOT cleared by {@link #onPageComplete},
     * allowing {@link MapContractOperator} to read it even after the page has been
     * completed.
     */
    private int lastStartedPageSize;
    /**
     * Maps page ID (from {@link #pageGeneration}) to position count.  Populated on
     * each {@link #onPageStart} call.  Allows {@link MapContractOperator} to look up
     * the size of any page by its embedded ID, even after the tracker has advanced
     * past that page.
     */
    private final java.util.HashMap<Integer, Integer> pageSizes = new java.util.HashMap<>();

    /**
     * Called by {@link MapExpandOperator} at the start of each source page.
     *
     * @throws IllegalStateException if a page is already active (i.e. {@link #onPageComplete}
     *                               has not been called for the previous page)
     */
    public void onPageStart(Page page) {
        if (active) {
            throw new IllegalStateException("onPageStart called while a page is already active");
        }
        this.currentPage = page;
        this.positionCount = page.getPositionCount();
        this.lastStartedPageSize = page.getPositionCount();
        this.active = true;
        this.pageGeneration++;
        this.pageSizes.put((int) this.pageGeneration, page.getPositionCount());
    }

    /**
     * Returns the position count of the current source page.
     *
     * @throws IllegalStateException if no page is currently active
     */
    public int currentPageSize() {
        if (active == false) {
            throw new IllegalStateException("currentPageSize called when no page is active");
        }
        return positionCount;
    }

    /**
     * Returns the current source {@link Page}.
     *
     * @throws IllegalStateException if no page is currently active
     */
    public Page currentSourcePage() {
        if (active == false) {
            throw new IllegalStateException("currentSourcePage called when no page is active");
        }
        return currentPage;
    }

    /**
     * Returns the current page generation — a monotonically increasing counter
     * incremented by each call to {@link #onPageStart}.
     * <p>
     *     This can be read at any time (including when no page is active) and is used
     *     by {@link MapContractOperator} to detect page boundaries.
     * </p>
     */
    public long currentPageGeneration() {
        return pageGeneration;
    }

    /**
     * Returns the position count of the most recently started source page.
     * <p>
     *     Unlike {@link #currentPageSize()}, this method is safe to call at any time
     *     (including after {@link #onPageComplete()}) and is intended for use by
     *     {@link MapContractOperator} when it needs the source page size but the
     *     expand operator has already completed the page.
     * </p>
     */
    public int lastStartedPageSize() {
        return lastStartedPageSize;
    }

    /**
     * Returns the position count of the source page with the given page ID.
     * <p>
     *     The page ID is the value written into the {@code _map_page_id} channel of each
     *     expanded row by {@link MapExpandOperator}.  This method is safe to call at any
     *     time and allows {@link MapContractOperator} to look up the size of any past
     *     source page.
     * </p>
     *
     * @throws IllegalArgumentException if no page with the given ID has been registered
     */
    public int lastStartedPageSize(int pageId) {
        Integer size = pageSizes.get(pageId);
        if (size == null) {
            throw new IllegalArgumentException("unknown page id: " + pageId);
        }
        return size;
    }

    /**
     * Called by {@link MapExpandOperator} after emitting all expanded rows for the
     * current source page.
     * <p>
     *     This method does <em>not</em> release the page; the caller retains ownership.
     * </p>
     *
     * @throws IllegalStateException if no page is currently active
     */
    public void onPageComplete() {
        if (active == false) {
            throw new IllegalStateException("onPageComplete called when no page is active");
        }
        currentPage = null;
        positionCount = 0;
        active = false;
    }
}
