/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.apache.lucene.util.automaton.UTF32ToUTF8;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.AutomataMatch;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;

/**
 * The multivalue analogue of {@link AutomataMatch}: matches a whole position's worth of
 * {@link BytesRef}s against an {@link Automaton} under an <em>any-value</em> reduction.
 * <p>
 * Where {@link AutomataMatch} answers "does this value match", this answers "does <em>any</em> value
 * of this (possibly multivalued) field match" — the pattern-matching analogue of how {@code mv_contains}
 * reduces equality, and the semantics a Lucene {@code wildcard}/{@code regexp} query already has over a
 * multivalued field.
 * <p>
 * The reduction is two-valued: a position with no values (a null or empty field) matches nothing and
 * yields {@code false} rather than {@code null}, so the predicate composes through {@code AND}/{@code OR}/{@code NOT}.
 * That is why the kernel takes the block directly via {@link Position} and is annotated
 * {@code allNullsIsNull = false} — the generated evaluator must not short-circuit a valueless position to null.
 */
public class MvAutomataMatch {
    /**
     * Build an {@link ExpressionEvaluator.Factory} that matches any value of a position against
     * {@code utf32Automaton}.
     */
    public static ExpressionEvaluator.Factory toEvaluator(Source source, ExpressionEvaluator.Factory field, Automaton utf32Automaton) {
        /*
         * Convert to UTF-8 ourselves rather than letting ByteRunAutomaton do it, so the automaton we
         * hand to toDot is the one actually being run — same reasoning as AutomataMatch.
         */
        Automaton automaton;
        try {
            automaton = Operations.determinize(new UTF32ToUTF8().convert(utf32Automaton), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        } catch (TooComplexToDeterminizeException e) {
            throw new IllegalArgumentException("Pattern was too complex to determinize", e);
        }

        ByteRunAutomaton run = new ByteRunAutomaton(automaton, true);
        return new MvAutomataMatchEvaluator.Factory(source, field, run, AutomataMatch.toDot(automaton), context -> new BytesRef());
    }

    @Evaluator(allNullsIsNull = false)
    static boolean process(
        @Position int position,
        BytesRefBlock field,
        @Fixed(includeInToString = false) ByteRunAutomaton automaton,
        @Fixed String pattern,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch
    ) {
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        for (int i = start; i < start + count; i++) {
            BytesRef v = field.getBytesRef(i, scratch);
            if (automaton.run(v.bytes, v.offset, v.length)) {
                return true;
            }
        }
        return false;
    }
}
