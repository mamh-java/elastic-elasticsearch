/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.core.Releasable;

/**
 * Shared state between two cooperating {@link Operator}s that run inside the same {@link Driver}.
 * <p>
 *     Unlike {@link SideChannel}, a {@code DriverLocalChannel} is scoped to a single Driver
 *     assembly: it is created once inside
 *     {@link Operator.OperatorFactory#get(DriverContext)} and passed directly to both
 *     operator constructors. Because both operators execute on the same thread (the Driver
 *     loop), no thread-safety machinery is required.
 * </p>
 * <p>
 *     The default {@link #close()} implementation is a no-op. Subclasses that hold
 *     releasable resources should override it.
 * </p>
 */
public abstract class DriverLocalChannel implements Releasable {

    @Override
    public void close() {
        // no-op by default; subclasses override when they hold resources
    }
}
