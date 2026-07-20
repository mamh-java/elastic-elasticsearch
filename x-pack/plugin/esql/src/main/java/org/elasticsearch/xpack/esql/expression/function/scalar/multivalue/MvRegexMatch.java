/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Shared base for the any-value pattern-matching functions {@link MvLike} (wildcard) and {@link MvRLike} (regex).
 * Both answer "does <em>any</em> value of a (possibly multivalued) field match this constant pattern", both are
 * two-valued ({@link Nullability#FALSE} — a null or empty field yields {@code false}, never {@code null}, so the
 * predicate composes through {@code AND}/{@code OR}/{@code NOT}), and both push to a bare Lucene multi-term query
 * that is existential over the field's terms.
 * <p>
 * This mirrors how the scalar operators {@code WildcardLike}/{@code RLike} share {@code RegexMatch}: the two
 * grammars compile to different Lucene queries and different evaluators, but the type-resolution, two-valued
 * folding, and pushdown gating are identical, so they live here once. Subclasses supply the pattern grammar
 * ({@link #validatePattern}), the evaluator ({@link #buildEvaluator}), and the pushed query ({@link #asQuery}).
 */
public abstract class MvRegexMatch extends BinaryScalarFunction implements EvaluatorMapper, TranslationAware {

    protected MvRegexMatch(Source source, Expression field, Expression pattern) {
        super(source, field, pattern);
    }

    protected MvRegexMatch(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        // A null-typed argument on either side is a valid signature: the whole predicate folds to false, per the
        // two-valued contract — a null field has no value to match, and a null pattern matches nothing.
        if (left().dataType() != DataType.NULL) {
            TypeResolution resolution = isString(left(), sourceText(), FIRST);
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        if (right().dataType() == DataType.NULL) {
            return TypeResolution.TYPE_RESOLVED;
        }

        TypeResolution resolution = isString(right(), sourceText(), SECOND).and(isFoldable(right(), sourceText(), SECOND));
        if (resolution.unresolved()) {
            return resolution;
        }

        // The pattern is a string type and foldable, but a *multivalued* string constant (e.g. ["a*", "b*"]) also
        // satisfies both — and folds to a List, which BytesRefs.toString renders as garbage that happens to be a valid
        // pattern. Reject anything that is not a single scalar string before it silently matches nothing.
        Object folded = right().fold(FoldContext.small());
        if (folded == null) {
            // A well-typed literal that holds null matches nothing; it folds to false rather than erroring.
            return TypeResolution.TYPE_RESOLVED;
        }
        if (folded instanceof BytesRef == false && folded instanceof String == false) {
            return new TypeResolution(
                "second argument of [" + sourceText() + "] must be a single pattern string, found value [" + folded + "]"
            );
        }

        // Build the pattern here so a malformed one is an analysis-time error rather than a planner crash. The regex
        // grammar can also blow past the determinize work limit, which throws TooComplexToDeterminizeException — a
        // RuntimeException that is neither of the other two, so it must be caught explicitly.
        try {
            validatePattern(BytesRefs.toString(folded));
        } catch (InvalidArgumentException | IllegalArgumentException | TooComplexToDeterminizeException e) {
            return new TypeResolution("Invalid pattern [" + BytesRefs.toString(folded) + "] for [" + sourceText() + "]: " + e.getMessage());
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    /** The folded pattern as a string, or {@code null} if the pattern folds to null. Only valid after type resolution. */
    protected final String patternString() {
        return BytesRefs.toString(right().fold(FoldContext.small()));
    }

    @Override
    public final DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public final Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public final Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public final ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // A null field has no values to match and a null pattern matches nothing, so either makes the predicate
        // constant false — whether the null arrives as a null-typed argument or as a well-typed literal holding null.
        if (left().dataType() == DataType.NULL || right().dataType() == DataType.NULL || patternString() == null) {
            return ConstantEvaluators.CONSTANT_FALSE_FACTORY;
        }
        return buildEvaluator(toEvaluator, patternString());
    }

    /**
     * The exactness argument, shared by both functions: a Lucene multi-term query on a keyword field is inherently
     * existential — it matches a document iff <em>some</em> indexed term of the field matches. That is this predicate's
     * definition verbatim (any value matches, and a missing field has no terms and so does not match, agreeing with the
     * two-valued contract), so the bare query <em>is</em> the predicate: {@link Translatable#YES}, the filter is
     * dropped, and {@code must_not} is an exact negation.
     * <p>
     * Nothing wraps the query in {@code SingleValueQuery}. That wrap gives single-value scalars their null-on-multivalue
     * semantics and would restrict the match to single-valued documents — the bug these functions exist to avoid. The
     * avoidance is structural: this class implements plain {@link TranslationAware}, never
     * {@link TranslationAware.SingleValueTranslationAware}, so {@code TranslatorHandler.asQuery} can never apply the wrap.
     * <p>
     * One caveat inherited verbatim from {@code WildcardLike}/{@code RLike}: on a keyword field with a {@code normalizer}
     * the pushed query normalizes the pattern server-side while the evaluator matches the raw pattern against the
     * normalized doc value, so the two can disagree. ES|QL currently reports such fields as pushable (the index resolver
     * hard-codes {@code normalized = false}), so this is not corrected here; it is the same shared upstream limitation
     * the scalar operators carry.
     */
    @Override
    public final Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        // text is excluded outright, even where an exact `.keyword` subfield exists. The analyzed field's terms are
        // tokens rather than whole values, and routing to the subfield inherits that subfield's ignore_above hole:
        // values the subfield ignored are matched by the evaluator but invisible to the pushed query — an under-match
        // that RECHECK cannot repair, since a recheck can only drop surfaced rows, never restore missing ones.
        if (left().dataType() == DataType.TEXT) {
            return Translatable.NO;
        }
        // An absent or null pattern folds the predicate to false long before this; there is no query to build.
        if (right().dataType() == DataType.NULL || patternString() == null) {
            return Translatable.NO;
        }
        if (patternPushable(patternString()) == false) {
            return Translatable.NO;
        }
        return pushdownPredicates.isPushableFieldAttribute(left()) ? Translatable.YES : Translatable.NO;
    }

    /**
     * The pushed field name. TEXT is already excluded by {@link #translatable}, so the field is always an exact keyword
     * and no {@code exactAttribute()} redirection is needed.
     */
    protected final String pushdownFieldName(TranslatorHandler handler) {
        LucenePushdownPredicates.checkIsPushableAttribute(left());
        return handler.nameOf(left());
    }

    /**
     * Validate the folded pattern, throwing {@link InvalidArgumentException}, {@link IllegalArgumentException} or
     * {@link TooComplexToDeterminizeException} if it is malformed. Called only with a non-null scalar pattern string.
     */
    protected abstract void validatePattern(String pattern);

    /** Whether a given (present, non-null) pattern can be pushed to Lucene. Defaults to true; {@link MvLike} refuses the empty pattern. */
    protected boolean patternPushable(String pattern) {
        return true;
    }

    /** Build the per-row evaluator for a present, non-null pattern. */
    protected abstract ExpressionEvaluator.Factory buildEvaluator(ToEvaluator toEvaluator, String pattern);
}
