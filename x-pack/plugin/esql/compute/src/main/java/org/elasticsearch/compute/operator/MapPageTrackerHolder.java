/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Vends a single {@link MapPageTracker} per {@link DriverContext} and shares it between the paired
 * {@link MapExpandOperator} and {@link MapContractOperator} of the same Driver.
 * <p>
 *     One holder is created at planning time and referenced by both operator factories. Because
 *     operator factories are shared across all Drivers that run the same pipeline, the tracker must be
 *     created lazily per Driver rather than eagerly at planning time — otherwise a single tracker would
 *     be shared (and double-closed) across concurrent Drivers, corrupting circuit-breaker accounting.
 * </p>
 * <p>
 *     The tracker is registered as a Driver-local {@link org.elasticsearch.core.Releasable} the first
 *     time it is requested for a Driver, so it is closed exactly once when that Driver finishes.
 * </p>
 */
public final class MapPageTrackerHolder {

    private final Map<DriverContext, MapPageTracker> perDriver = new ConcurrentHashMap<>();

    /**
     * Returns the {@link MapPageTracker} for the given Driver, creating and registering it on first use.
     */
    public MapPageTracker get(DriverContext driverContext) {
        return perDriver.computeIfAbsent(driverContext, ctx -> {
            MapPageTracker tracker = new MapPageTracker(ctx.blockFactory().breaker());
            ctx.addReleasable(tracker);
            return tracker;
        });
    }
}
