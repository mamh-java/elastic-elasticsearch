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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node for the contract phase of the MAP command.
 * <p>
 *     Re-collapses the expanded rows produced by {@link MapExpandExec} back into one output row
 *     per original document position. The {@code _map_pos} channel identifies which source
 *     position each expanded row belongs to; the RETURNING channel value is merged into a single
 *     (possibly multi-valued) output column. The {@code _map_pos}, {@code _map_page_id}, and
 *     {@code _map_col_<name>} channels are stripped; output is the source columns followed by
 *     RETURNING.
 * </p>
 * <p>
 *     This node references its paired {@link MapExpandExec} so the planner can share the single
 *     {@code MapPageTracker} created for the expand phase within the same Driver.
 * </p>
 * <p>
 *     Physical plans are not serialized, so this node does not implement
 *     {@link #writeTo} or {@link #getWriteableName}.
 * </p>
 */
public class MapContractExec extends UnaryExec {

    private final MapExpandExec expandExec;
    private final Attribute returningAttr;
    private final List<Attribute> sourceAttributes;
    private final List<Attribute> output;

    public MapContractExec(
        Source source,
        PhysicalPlan child,
        MapExpandExec expandExec,
        Attribute returningAttr,
        List<Attribute> sourceAttributes,
        List<Attribute> output
    ) {
        super(source, child);
        this.expandExec = expandExec;
        this.returningAttr = returningAttr;
        this.sourceAttributes = sourceAttributes;
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

    /**
     * The paired expand node, used by the planner to look up the shared {@code MapPageTracker}.
     */
    public MapExpandExec expandExec() {
        return expandExec;
    }

    public Attribute returningAttr() {
        return returningAttr;
    }

    /**
     * The original source columns passed through to the contracted output, in output order.
     */
    public List<Attribute> sourceAttributes() {
        return sourceAttributes;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected AttributeSet computeReferences() {
        // The contract operator reads the source columns (passed through), the RETURNING column, and
        // the synthetic _map_pos / _map_page_id / _map_col_* channels emitted by the paired expand
        // node. Declaring these keeps them alive through column pruning.
        List<Attribute> refs = new ArrayList<>(sourceAttributes);
        refs.add(returningAttr);
        refs.add(expandExec.mapPosAttr());
        refs.add(expandExec.mapPageIdAttr());
        refs.addAll(expandExec.mapColAttributes());
        return AttributeSet.of(refs);
    }

    @Override
    public MapContractExec replaceChild(PhysicalPlan newChild) {
        return new MapContractExec(source(), newChild, expandExec, returningAttr, sourceAttributes, output);
    }

    @Override
    protected NodeInfo<MapContractExec> info() {
        return NodeInfo.create(this, MapContractExec::new, child(), expandExec, returningAttr, sourceAttributes, output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), expandExec, returningAttr, sourceAttributes, output);
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
            && Objects.equals(expandExec, other.expandExec)
            && Objects.equals(returningAttr, other.returningAttr)
            && Objects.equals(sourceAttributes, other.sourceAttributes)
            && Objects.equals(output, other.output);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[returningAttr=" + returningAttr + "]";
    }
}
