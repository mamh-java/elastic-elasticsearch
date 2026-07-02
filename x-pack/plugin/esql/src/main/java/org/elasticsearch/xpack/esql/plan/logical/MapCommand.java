/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.MapCombinator;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node for the MAP command.
 * <p>
 *     Syntax: {@code MAP <combinator-expr> RETURNING result [ <sub-pipeline> ]}
 * </p>
 * <p>
 *     The combinator describes how the input multi-valued columns are expanded into a
 *     "mini-table" of combination rows; the sub-pipeline is then applied to each
 *     mini-table; the RETURNING column is extracted and collapsed back into one
 *     (possibly multi-valued) output row per source document.
 * </p>
 * <p>
 *     This node is not serialized; it is a snapshot-only, coordinator-side logical plan node.
 * </p>
 */
public class MapCommand extends UnaryPlan implements TelemetryAware {

    private final MapCombinator combinator;
    private final Attribute returningAttr;
    private final LogicalPlan subPipeline;
    private List<Attribute> lazyOutput;

    public MapCommand(Source source, LogicalPlan child, MapCombinator combinator, Attribute returningAttr, LogicalPlan subPipeline) {
        super(source, child);
        this.combinator = combinator;
        this.returningAttr = returningAttr;
        this.subPipeline = subPipeline;
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

    public Attribute returningAttr() {
        return returningAttr;
    }

    public LogicalPlan subPipeline() {
        return subPipeline;
    }

    public MapCommand withSubPipeline(LogicalPlan newSubPipeline) {
        return new MapCommand(source(), child(), combinator, returningAttr, newSubPipeline);
    }

    public MapCommand withCombinatorAndReturning(MapCombinator newCombinator, Attribute newReturningAttr) {
        return new MapCommand(source(), child(), newCombinator, newReturningAttr, subPipeline);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<Attribute> out = new ArrayList<>(child().output());
            out.add(returningAttr);
            lazyOutput = out;
        }
        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        // References include the combinator leaves (resolved against child output)
        // and the sub-pipeline input columns.
        return AttributeSet.EMPTY;
    }

    @Override
    public boolean expressionsResolved() {
        if (returningAttr.resolved() == false) {
            return false;
        }
        // Check all combinator leaf columns are resolved by verifying they reference valid attributes
        for (MapCombinator.Leaf leaf : combinator.leaves()) {
            if (leaf.channel() < 0) {
                return false;
            }
        }
        return subPipeline != null && subPipeline.resolved();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new MapCommand(source(), newChild, combinator, returningAttr, subPipeline);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, MapCommand::new, child(), combinator, returningAttr, subPipeline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), combinator, returningAttr, subPipeline);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MapCommand other = (MapCommand) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(combinator, other.combinator)
            && Objects.equals(returningAttr, other.returningAttr)
            && Objects.equals(subPipeline, other.subPipeline);
    }
}
