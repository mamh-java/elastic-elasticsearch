/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared base for coordinator-only joins whose right ("build") side is an independent subquery that must be executed first, its result
 * buffered into a {@link LocalRelation}, and only then joined against the streaming left ("probe") side. Concretely this covers the
 * {@code field IN (subquery)} family ({@link SubqueryHashJoin} and its {@link SemiJoin}/{@link AntiJoin}/{@link MarkJoin} subtypes) and the
 * PromQL vector-matching INNER equi-join ({@code EqJoin}).
 * <p>
 * Unlike {@link InlineJoin}, the right side does not embed the left via a {@code StubRelation}; it is a self-contained subquery, so no stub
 * replacement or deep copy is needed and the node executed as the subplan is the very same instance held on the join's right, which doubles
 * as the identity key that {@link #newMainPlan} matches against.
 * <p>
 * The coordinator phase loop (see {@code EsqlSession}) drives this base directly: {@link #firstSubPlan} yields the next unmaterialized right
 * subquery, and once its result is available {@link #newMainPlan} substitutes it back via the per-subtype {@link #onMaterializedRight} hook
 * ({@link SubqueryHashJoin} rewrites into an {@code IN} list or hash join; {@code EqJoin} simply {@code replaceRight}s the local relation so
 * the mapper can lower it to an {@code EqJoinExec}).
 * <p>
 * These nodes are ephemeral: they are resolved away on the coordinator before the physical plan crosses the wire, so they are never
 * serialized (see {@link #writeTo}).
 */
public abstract class AbstractHashJoin extends Join implements SortPreserving, ExecutesOn.Coordinator {

    protected AbstractHashJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config, false);
    }

    protected AbstractHashJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields
    ) {
        super(source, left, right, type, leftFields, rightFields, null, false);
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
     * Substitutes the materialized right-side subquery result back into this node. {@link SubqueryHashJoin} converts it into an {@code IN}
     * list or a hash join (using {@code ctx}); {@code EqJoin} simply replaces its right child with {@code data}.
     */
    protected abstract LogicalPlan onMaterializedRight(LocalRelation data, MaterializationContext ctx);

    /**
     * Finds the first (bottom-up) join in the plan whose right subquery has not yet been replaced with results, and returns it as the next
     * subplan to execute. Bottom-up ordering ensures nested subqueries are resolved before the outer ones that depend on them.
     */
    public static LogicalPlanTuple firstSubPlan(LogicalPlan optimizedPlan, Set<LocalRelation> subPlansResults) {
        Holder<LogicalPlan> subPlanHolder = new Holder<>();
        optimizedPlan.forEachUp(AbstractHashJoin.class, join -> {
            if (subPlanHolder.get() == null) {
                if (join.right() instanceof LocalRelation lr && subPlansResults.contains(lr)) {
                    return;
                }
                subPlanHolder.set(join.right());
            }
        });
        LogicalPlan subPlan = subPlanHolder.get();
        if (subPlan == null) {
            return null;
        }
        subPlan.setOptimized();
        // The subplan is the very same instance held on the join's right side, so it doubles as the identity key that newMainPlan matches
        // against - hence both tuple slots are the same.
        return new LogicalPlanTuple(subPlan, subPlan);
    }

    /**
     * Rebuilds the main plan once the subquery result is available, substituting it into the matching join via {@link #onMaterializedRight}.
     * The match is by object identity ({@code right() == originalSubPlan}) rather than equality.
     */
    public static LogicalPlan newMainPlan(
        LogicalPlan optimizedPlan,
        LogicalPlanTuple subPlans,
        LocalRelation resultWrapper,
        MaterializationContext ctx
    ) {
        LogicalPlan newPlan = optimizedPlan.transformUp(
            AbstractHashJoin.class,
            join -> join.right() == subPlans.originalSubPlan() ? join.onMaterializedRight(resultWrapper, ctx) : join
        );
        newPlan.setOptimized();
        return newPlan;
    }

    /**
     * Tuple holding the subplan to execute and the original plan node used as the identity key when substituting the result back.
     */
    public record LogicalPlanTuple(LogicalPlan subPlan, LogicalPlan originalSubPlan) {}

    /**
     * Resources the coordinator supplies to {@link #onMaterializedRight}. Only {@link SubqueryHashJoin} uses them (to decide between the
     * {@code IN}-list and hash-join rewrites and to manage the dedup page lifecycle); {@code EqJoin} ignores them.
     */
    public record MaterializationContext(int hashJoinThreshold, BlockFactory blockFactory, AtomicReference<Page> pageHolder) {}
}
