/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.MapCombinator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node for the expand phase of the MAP command.
 * <p>
 *     Expands each document position into a set of combination rows according to the
 *     {@link MapCombinator} tree. The expanded output layout is, in order:
 * </p>
 * <ol>
 *     <li>all original source channels (this node's child output), broadcast per row;</li>
 *     <li>one {@code _map_col_<name>} channel per combinator leaf (the scalar value for the row);</li>
 *     <li>a {@code _map_pos} channel (the originating source position); and</li>
 *     <li>a {@code _map_page_id} channel (source-page identifier used by the contract phase).</li>
 * </ol>
 * <p>
 *     The {@code _map_col_<name>} attributes carried here are the exact synthetic attributes
 *     the analyzer created for the sub-pipeline, so the sub-pipeline operators planned on top of
 *     this node resolve their input channels correctly.
 * </p>
 * <p>
 *     Physical plans are not serialized, so this node does not implement
 *     {@link #writeTo} or {@link #getWriteableName}.
 * </p>
 */
public class MapExpandExec extends UnaryExec {

    private final MapCombinator combinator;
    private final List<String> leafNames;
    private final List<Attribute> leafSourceAttributes;
    private final List<Attribute> mapColAttributes;
    private final Attribute mapPosAttr;
    private final Attribute mapPageIdAttr;
    private final List<Attribute> output;

    public MapExpandExec(
        Source source,
        PhysicalPlan child,
        MapCombinator combinator,
        List<String> leafNames,
        List<Attribute> leafSourceAttributes,
        List<Attribute> mapColAttributes,
        Attribute mapPosAttr,
        Attribute mapPageIdAttr,
        List<Attribute> output
    ) {
        super(source, child);
        this.combinator = combinator;
        this.leafNames = leafNames;
        this.leafSourceAttributes = leafSourceAttributes;
        this.mapColAttributes = mapColAttributes;
        this.mapPosAttr = mapPosAttr;
        this.mapPageIdAttr = mapPageIdAttr;
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

    public MapCombinator combinator() {
        return combinator;
    }

    public List<String> leafNames() {
        return leafNames;
    }

    /**
     * The original source columns referenced by the combinator leaves, in leaf order.
     * Used at planning time to resolve the input channels the expand operator reads from.
     */
    public List<Attribute> leafSourceAttributes() {
        return leafSourceAttributes;
    }

    /**
     * The synthetic {@code _map_col_<name>} attributes emitted by the expand operator, in
     * leaf order. These are the same attributes the analyzer wired into the sub-pipeline.
     */
    public List<Attribute> mapColAttributes() {
        return mapColAttributes;
    }

    public Attribute mapPosAttr() {
        return mapPosAttr;
    }

    public Attribute mapPageIdAttr() {
        return mapPageIdAttr;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected AttributeSet computeReferences() {
        // The expand operator reads the combinator leaf columns from its child; declaring them here
        // keeps those columns alive across the upstream exchange and prevents column pruning from
        // dropping them before they reach the coordinator.
        return AttributeSet.of(leafSourceAttributes);
    }

    @Override
    public MapExpandExec replaceChild(PhysicalPlan newChild) {
        return new MapExpandExec(
            source(),
            newChild,
            combinator,
            leafNames,
            leafSourceAttributes,
            mapColAttributes,
            mapPosAttr,
            mapPageIdAttr,
            output
        );
    }

    @Override
    protected NodeInfo<MapExpandExec> info() {
        return NodeInfo.create(
            this,
            MapExpandExec::new,
            child(),
            combinator,
            leafNames,
            leafSourceAttributes,
            mapColAttributes,
            mapPosAttr,
            mapPageIdAttr,
            output
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), combinator, leafNames, leafSourceAttributes, mapColAttributes, mapPosAttr, mapPageIdAttr, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MapExpandExec other = (MapExpandExec) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(combinator, other.combinator)
            && Objects.equals(leafNames, other.leafNames)
            && Objects.equals(leafSourceAttributes, other.leafSourceAttributes)
            && Objects.equals(mapColAttributes, other.mapColAttributes)
            && Objects.equals(mapPosAttr, other.mapPosAttr)
            && Objects.equals(mapPageIdAttr, other.mapPageIdAttr)
            && Objects.equals(output, other.output);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[combinator=" + combinator + ", mapColAttributes=" + mapColAttributes + "]";
    }

    /**
     * Builds the output schema for the expand node: child output, followed by the synthetic
     * {@code _map_col_<name>} leaf columns, the {@code _map_pos} channel, and the
     * {@code _map_page_id} channel. The ordering matches the channel layout produced by
     * {@link org.elasticsearch.compute.operator.MapExpandOperator}.
     *
     * @param childOutput     the output attributes of the upstream child plan
     * @param mapColAttributes the synthetic {@code _map_col_<name>} attributes
     * @param mapPosAttr      the synthetic {@code _map_pos} attribute
     * @param mapPageIdAttr   the synthetic {@code _map_page_id} attribute
     */
    public static List<Attribute> buildOutput(
        List<Attribute> childOutput,
        List<Attribute> mapColAttributes,
        Attribute mapPosAttr,
        Attribute mapPageIdAttr
    ) {
        List<Attribute> out = new ArrayList<>(childOutput.size() + mapColAttributes.size() + 2);
        out.addAll(childOutput);
        out.addAll(mapColAttributes);
        out.add(mapPosAttr);
        out.add(mapPageIdAttr);
        return out;
    }
}
