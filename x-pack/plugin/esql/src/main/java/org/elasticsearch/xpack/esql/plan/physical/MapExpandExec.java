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
 * Physical plan node stub for the expand phase of the MAP command.
 * <p>
 *     Expands each document position into a set of combination rows according to the
 *     {@link MapCombinator} tree. The expanded output includes the original source channels,
 *     one {@code _map_col_<name>} channel per leaf column, and a {@code _map_pos} channel.
 * </p>
 * <p>
 *     Physical plans are not serialized, so this node does not implement
 *     {@link #writeTo} or {@link #getWriteableName}.
 * </p>
 */
public class MapExpandExec extends UnaryExec {

    private final MapCombinator combinator;
    private final List<String> leafNames;
    private final int mapPosChannel;
    private final List<Attribute> output;

    public MapExpandExec(
        Source source,
        PhysicalPlan child,
        MapCombinator combinator,
        List<String> leafNames,
        int mapPosChannel,
        List<Attribute> output
    ) {
        super(source, child);
        this.combinator = combinator;
        this.leafNames = leafNames;
        this.mapPosChannel = mapPosChannel;
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

    public int mapPosChannel() {
        return mapPosChannel;
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
    public MapExpandExec replaceChild(PhysicalPlan newChild) {
        return new MapExpandExec(source(), newChild, combinator, leafNames, mapPosChannel, output);
    }

    @Override
    protected NodeInfo<MapExpandExec> info() {
        return NodeInfo.create(this, MapExpandExec::new, child(), combinator, leafNames, mapPosChannel, output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), combinator, leafNames, mapPosChannel, output);
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
            && mapPosChannel == other.mapPosChannel
            && Objects.equals(output, other.output);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[combinator=" + combinator + ", mapPosChannel=" + mapPosChannel + "]";
    }

    /**
     * Returns a new list containing child output plus the expanded leaf columns and the
     * {@code _map_pos} channel attribute, building the output schema for this exec node.
     *
     * @param childOutput    the output attributes of the upstream child plan
     * @param leafAttributes the synthetic {@code _map_col_<name>} attributes
     * @param mapPosAttr     the synthetic {@code _map_pos} attribute
     */
    public static List<Attribute> buildOutput(List<Attribute> childOutput, List<Attribute> leafAttributes, Attribute mapPosAttr) {
        List<Attribute> out = new ArrayList<>(childOutput.size() + leafAttributes.size() + 1);
        out.addAll(childOutput);
        out.addAll(leafAttributes);
        out.add(mapPosAttr);
        return out;
    }
}
