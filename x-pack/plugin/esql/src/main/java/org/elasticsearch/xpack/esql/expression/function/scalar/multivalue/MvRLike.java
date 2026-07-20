/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RegexQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Any-value regular-expression matching over a multivalued field: returns {@code true} when <em>any</em> value of
 * {@code field} matches {@code pattern}.
 * <p>
 * The regex counterpart of {@link MvLike}, and the second half of what the out-of-band Query DSL filter needs: a
 * {@code regexp} clause is a different grammar compiling to a different Lucene query than {@code wildcard}, so it
 * gets its own function rather than a mode flag on {@code mv_like}. Same two-valued contract — a null or empty field
 * yields {@code false}, never {@code null}.
 * <p>
 * There are no affix fast paths here: unlike a wildcard pattern, a regular expression has no prefix/suffix/contains
 * shapes to peel off, so every pattern runs the automaton.
 */
public class MvRLike extends BinaryScalarFunction implements EvaluatorMapper, TranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvRLike", MvRLike::new);

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvRLike.class).binary(MvRLike::new).name("mv_rlike");

    @FunctionInfo(
        returnType = "boolean",
        briefSummary = "Checks if any value of a multivalue field matches a regular expression.",
        description = "Returns `true` when any value yielded by `field` matches `pattern`, using the same regular-expression "
            + "syntax as <<esql-rlike-operator,`RLIKE`>>. Unlike `RLIKE`, which is a single-value scalar, this reduces over "
            + "every value of a multivalue field. A null or empty `field` returns `false`.",
        examples = {
            @Example(file = "string", tag = "mv_rlike"),
            @Example(description = "Character classes work as they do in `RLIKE`:", file = "string", tag = "mv_rlike_class") },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.6.0") }
    )
    public MvRLike(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text" },
            description = "Multivalue expression to test. If null or empty, the function returns `false`."
        ) Expression field,
        @Param(
            name = "pattern",
            type = { "keyword", "text" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Regular expression. Must be a constant. The pattern must match a value in full, as with `RLIKE`."
        ) Expression pattern
    ) {
        super(source, field, pattern);
    }

    private MvRLike(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
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
        if (patternString() == null) {
            return TypeResolution.TYPE_RESOLVED;
        }

        /*
         * RLikePattern's constructor does not parse the regex — RegExp is only built inside doCreateAutomaton — so an
         * unparseable pattern would otherwise surface as a planner crash. Probing the automaton here turns it into an
         * analysis-time error.
         */
        try {
            pattern().createAutomaton(false);
        } catch (InvalidArgumentException | IllegalArgumentException e) {
            return new TypeResolution("Invalid pattern [" + patternString() + "] for [" + sourceText() + "]: " + e.getMessage());
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    private String patternString() {
        return BytesRefs.toString(right().fold(FoldContext.small()));
    }

    private RLikePattern pattern() {
        return new RLikePattern(patternString());
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected MvRLike replaceChildren(Expression newLeft, Expression newRight) {
        return new MvRLike(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvRLike::new, left(), right());
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (left().dataType() == DataType.NULL || right().dataType() == DataType.NULL || patternString() == null) {
            return ConstantEvaluators.CONSTANT_FALSE_FACTORY;
        }
        return MvAutomataMatch.toEvaluator(source(), toEvaluator.apply(left()), pattern().createAutomaton(false));
    }

    /**
     * Same exactness argument as {@link MvLike#translatable}: a Lucene {@code regexp} query matches a document when any
     * of the field's terms matches, which is this predicate's definition, so the bare query is the predicate and
     * negation is exact. Plain {@link TranslationAware}, never {@code SingleValueTranslationAware} — the wrap would
     * restrict the match to single-valued documents.
     */
    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        // text is excluded for the same reason as mv_like: analyzed terms are tokens, and the exact subfield carries an
        // ignore_above hole that would make the pushed query under-match.
        if (left().dataType() == DataType.TEXT) {
            return Translatable.NO;
        }
        if (right().dataType() == DataType.NULL || patternString() == null) {
            return Translatable.NO;
        }
        return pushdownPredicates.isPushableFieldAttribute(left()) ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        Expression field = left();
        LucenePushdownPredicates.checkIsPushableAttribute(field);
        // RLikePattern is Lucene RegExp syntax already, and asJavaRegex returns it unchanged, so the pushed query
        // compiles the same pattern language the evaluator's automaton does.
        return new RegexQuery(
            source(),
            handler.nameOf(field instanceof FieldAttribute fa ? fa.exactAttribute() : field),
            pattern().asJavaRegex(),
            false
        );
    }
}
