/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node stub for the contract phase of the MAP command.
 * <p>
 *     Re-collapses the expanded rows produced by {@link MapExpandExec} back into one
 *     output row per original document position. The {@code _map_pos} channel identifies
 *     which source position each expanded row belongs to; the RETURNING channel value is
 *     merged into a single (possibly multi-valued) output column.
 * </p>
 * <p>
 *     Physical plans are not serialized, so this node does not implement
 *     {@link #writeTo} or {@link #getWriteableName}.
 * </p>
 */
public class MapContractExec extends UnaryExec {

    private final int mapPosChannel;
    private final int returningChannel;
    private final List<Attribute> output;

    public MapContractExec(Source source, PhysicalPlan child, int mapPosChannel, int returningChannel, List<Attribute> output) {
        super(source, child);
        this.mapPosChannel = mapPosChannel;
        this.returningChannel = returningChannel;
        this.output = output;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public int mapPosChannel() {
        return mapPosChannel;
    }

    public int returningChannel() {
        return returningChannel;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    public MapContractExec replaceChild(PhysicalPlan newChild) {
        return new MapContractExec(source(), newChild, mapPosChannel, returningChannel, output);
    }

    @Override
    protected NodeInfo<MapContractExec> info() {
        return NodeInfo.create(this, MapContractExec::new, child(), mapPosChannel, returningChannel, output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), mapPosChannel, returningChannel, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MapContractExec other = (MapContractExec) obj;
        return Objects.equals(child(), other.child())
            && mapPosChannel == other.mapPosChannel
            && returningChannel == other.returningChannel
            && Objects.equals(output, other.output);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[mapPosChannel=" + mapPosChannel + ", returningChannel=" + returningChannel + "]";
    }
}
