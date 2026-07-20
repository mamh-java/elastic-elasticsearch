/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.ByteMatchers;
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

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Any-value wildcard matching over a multivalued field: returns {@code true} when <em>any</em> value of
 * {@code field} matches {@code pattern}.
 * <p>
 * {@code LIKE} is a single-value scalar — applied to a multivalued field it yields {@code null} rather than
 * reducing to any-value semantics. This is the reduction, and it is the semantics a Lucene {@code wildcard}
 * query already has over a multivalued field: an index matches a document when any value of the field matches.
 * <p>
 * Two-valued ({@link Nullability#FALSE}): a null or empty field yields {@code false}, never {@code null}, so the
 * predicate composes through {@code AND}/{@code OR}/{@code NOT} the way {@code mv_contains} does.
 */
public class MvLike extends BinaryScalarFunction implements EvaluatorMapper, TranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvLike", MvLike::new);

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvLike.class).binary(MvLike::new).name("mv_like");

    @FunctionInfo(
        returnType = "boolean",
        briefSummary = "Checks if any value of a multivalue field matches a wildcard pattern.",
        description = "Returns `true` when any value yielded by `field` matches `pattern`, using the same wildcard syntax as "
            + "<<esql-like-operator,`LIKE`>>. Unlike `LIKE`, which is a single-value scalar, this reduces over every value of a "
            + "multivalue field. A null or empty `field` returns `false`.",
        examples = {
            @Example(file = "string", tag = "mv_like"),
            @Example(
                description = "A prefix pattern matches when any value starts with the literal:",
                file = "string",
                tag = "mv_like_prefix"
            ),
            @Example(description = "Because the result is never null, it composes under `NOT`:", file = "string", tag = "mv_like_not") },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.6.0") }
    )
    public MvLike(
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
            description = "Wildcard pattern. Must be a constant. `*` matches any run of characters, `?` matches a single "
                + "character; escape either with `\\`."
        ) Expression pattern
    ) {
        super(source, field, pattern);
    }

    private MvLike(StreamInput in) throws IOException {
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
        // A well-typed literal that happens to hold null matches nothing; it folds to false rather than erroring.
        if (patternString() == null) {
            return TypeResolution.TYPE_RESOLVED;
        }

        // Build the pattern here so a malformed one is an analysis-time error rather than a planner crash.
        try {
            pattern();
        } catch (InvalidArgumentException | IllegalArgumentException e) {
            return new TypeResolution("Invalid pattern [" + patternString() + "] for [" + sourceText() + "]: " + e.getMessage());
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    private String patternString() {
        return BytesRefs.toString(right().fold(FoldContext.small()));
    }

    private WildcardPattern pattern() {
        return new WildcardPattern(patternString());
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
    protected MvLike replaceChildren(Expression newLeft, Expression newRight) {
        return new MvLike(source(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvLike::new, left(), right());
    }

    @Override
    public Object fold(FoldContext ctx) {
        return EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // A null field has no values to match and a null pattern matches nothing, so either makes the predicate
        // constant false — whether the null arrives as a null-typed argument or as a well-typed literal holding null.
        if (left().dataType() == DataType.NULL || right().dataType() == DataType.NULL || patternString() == null) {
            return ConstantEvaluators.CONSTANT_FALSE_FACTORY;
        }
        WildcardPattern pattern = pattern();
        if (pattern.pattern().isEmpty()) {
            // The empty pattern accepts the empty string — same special case RegexMatch.toEvaluator makes, so a value
            // matches mv_like exactly when it would match LIKE.
            return MvAutomataMatch.toEvaluator(source(), toEvaluator.apply(left()), Automata.makeEmptyString());
        }
        /*
         * Affix-shaped patterns skip the automaton entirely, mirroring WildcardLike.toEvaluator. The saving is larger
         * here than for LIKE: the matcher runs once per *value*, not once per row, so replacing a per-byte
         * RunAutomaton.step walk with a byte compare (or a SIMD substring scan) is paid for on every value of every
         * multivalued position. `prefix*` is also the shape the DSL `prefix` clause compiles to, so the fast path lands
         * on the consumer's hottest case.
         *
         * WildcardLike delegates these to StartsWith/EndsWith; mv_like cannot, because those are single-value scalars
         * that null out on a multivalued field — the exact behaviour this function exists to avoid. So the reduction is
         * open-coded here over the same ByteMatchers primitives.
         */
        ExpressionEvaluator.Factory field = toEvaluator.apply(left());
        return switch (pattern.shape()) {
            case WildcardPattern.Shape.Prefix(String prefix) -> new MvLikePrefixEvaluator.Factory(
                source(),
                field,
                new BytesRef(prefix),
                context -> new BytesRef()
            );
            case WildcardPattern.Shape.Suffix(String suffix) -> new MvLikeSuffixEvaluator.Factory(
                source(),
                field,
                new BytesRef(suffix),
                context -> new BytesRef()
            );
            case WildcardPattern.Shape.Contains(String literal) -> new MvLikeContainsEvaluator.Factory(
                source(),
                field,
                new BytesRef(literal),
                context -> new BytesRef()
            );
            case WildcardPattern.Shape.General ignored -> MvAutomataMatch.toEvaluator(source(), field, pattern.createAutomaton(false));
        };
    }

    /**
     * A Lucene multi-term query on a keyword field is inherently existential: a {@code wildcard} query matches a
     * document iff <em>some</em> indexed term of the field matches the compiled automaton. That is {@code mv_like}'s
     * definition verbatim — any value matches, and a missing field has no terms and so does not match, agreeing with
     * the two-valued contract. The bare query <em>is</em> the predicate, so this is {@link Translatable#YES}: the
     * filter is dropped, and {@code must_not(wildcard)} is an exact negation.
     * <p>
     * Nothing here wraps the query in {@code SingleValueQuery}. That wrap exists to give single-value scalars their
     * null-on-multivalue semantics and would be a correctness bug for this predicate — it would match only
     * single-valued documents. The avoidance is structural rather than a flag: {@code TranslatorHandler.asQuery} wraps
     * only {@link TranslationAware.SingleValueTranslationAware} implementers, and this class deliberately implements
     * plain {@link TranslationAware}, exactly as {@code mv_contains} does.
     */
    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        /*
         * text is excluded outright, even where an exact `.keyword` subfield exists. The analyzed field's terms are
         * tokens rather than whole values, and routing to the subfield inherits that subfield's ignore_above hole:
         * values the subfield ignored are matched by the evaluator but invisible to the pushed query. That is an
         * under-match, which RECHECK cannot repair — a recheck can only drop surfaced rows, never restore missing ones.
         */
        if (left().dataType() == DataType.TEXT) {
            return Translatable.NO;
        }
        // A pattern that is absent or null folds the predicate to false long before this; there is no query to build.
        if (right().dataType() == DataType.NULL || patternString() == null) {
            return Translatable.NO;
        }
        /*
         * The empty pattern is not pushable. The evaluator maps it to Automata.makeEmptyString(), matching a value that
         * is the empty string — the same special case RegexMatch makes, so mv_like agrees with LIKE. A Lucene wildcard
         * query built from an empty pattern matches no term at all, so pushing it would drop documents whose field
         * holds an empty string. That is an under-match, which no recheck can repair, so the evaluator keeps it.
         * The pushed-vs-evaluator differential in EsqlActionIT is what caught this.
         */
        if (patternString().isEmpty()) {
            return Translatable.NO;
        }
        return pushdownPredicates.isPushableFieldAttribute(left()) ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        Expression field = left();
        LucenePushdownPredicates.checkIsPushableAttribute(field);
        return new WildcardQuery(
            source(),
            handler.nameOf(field instanceof FieldAttribute fa ? fa.exactAttribute() : field),
            pattern().asLuceneWildcard(),
            false,
            pushdownPredicates.flags().stringLikeOnIndex()
        );
    }

    /** Any value starts with {@code prefix} — the {@code literal*} shape. */
    @Evaluator(extraName = "Prefix", allNullsIsNull = false)
    static boolean processPrefix(
        @Position int position,
        BytesRefBlock field,
        @Fixed(jitConstant = true) BytesRef prefix,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) {
        int start = field.getFirstValueIndex(position);
        int end = start + field.getValueCount(position);
        for (int i = start; i < end; i++) {
            if (ByteMatchers.startsWith(field.getBytesRef(i, scratch), prefix)) {
                return true;
            }
        }
        return false;
    }

    /** Any value ends with {@code suffix} — the {@code *literal} shape. */
    @Evaluator(extraName = "Suffix", allNullsIsNull = false)
    static boolean processSuffix(
        @Position int position,
        BytesRefBlock field,
        @Fixed(jitConstant = true) BytesRef suffix,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) {
        int start = field.getFirstValueIndex(position);
        int end = start + field.getValueCount(position);
        for (int i = start; i < end; i++) {
            if (ByteMatchers.endsWith(field.getBytesRef(i, scratch), suffix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Any value contains {@code literal} — the {@code *literal*} shape. Routes through the SIMD substring primitive
     * {@link ByteMatchers#containsLiteral}, as {@code WildcardLike}'s Contains fast path does.
     */
    @Evaluator(extraName = "Contains", allNullsIsNull = false)
    static boolean processContains(
        @Position int position,
        BytesRefBlock field,
        @Fixed(jitConstant = true) BytesRef literal,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) {
        int start = field.getFirstValueIndex(position);
        int end = start + field.getValueCount(position);
        for (int i = start; i < end; i++) {
            if (ByteMatchers.containsLiteral(field.getBytesRef(i, scratch), literal)) {
                return true;
            }
        }
        return false;
    }
}
