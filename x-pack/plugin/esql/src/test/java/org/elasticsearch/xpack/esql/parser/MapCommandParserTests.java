/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.compute.operator.MapCombinator;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MapCommand;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Parser-level tests for the MAP command.
 * <p>
 *     MAP is gated behind the dev-version predicate in the grammar; the shared
 *     {@code TEST_PARSER} runs in snapshot mode, so these queries parse without extra setup.
 *     The tests assert the shape of the {@link MapCommand} logical node the parser builds:
 *     the combinator tree, the RETURNING attribute name, and the sub-pipeline plan.
 * </p>
 */
public class MapCommandParserTests extends AbstractStatementParserTests {

    private MapCommand parseMap(String mapClause) {
        LogicalPlan plan = processingCommand(mapClause);
        return as(plan, MapCommand.class);
    }

    public void testSingleLeaf() {
        MapCommand map = parseMap("MAP a RETURNING x [ EVAL x = a ]");
        MapCombinator.Leaf leaf = as(map.combinator(), MapCombinator.Leaf.class);
        assertEquals("a", leaf.name());
        // Channel is a placeholder until the analyzer resolves it against the child output.
        assertEquals(-1, leaf.channel());
        assertEquals("x", map.returningAttr().name());
        as(map.subPipeline(), Eval.class);
    }

    public void testCrossBindsTighterThanZip() {
        // a ZIP b CROSS c must parse as Zip(a, Cross(b, c)).
        MapCommand map = parseMap("MAP a ZIP b CROSS c RETURNING x [ EVAL x = a ]");
        MapCombinator.Zip zip = as(map.combinator(), MapCombinator.Zip.class);
        assertEquals("a", as(zip.left(), MapCombinator.Leaf.class).name());
        MapCombinator.Cross cross = as(zip.right(), MapCombinator.Cross.class);
        assertEquals("b", as(cross.left(), MapCombinator.Leaf.class).name());
        assertEquals("c", as(cross.right(), MapCombinator.Leaf.class).name());
    }

    public void testCrossIsLeftAssociative() {
        // a CROSS b CROSS c must parse as Cross(Cross(a, b), c).
        MapCommand map = parseMap("MAP a CROSS b CROSS c RETURNING x [ EVAL x = a ]");
        MapCombinator.Cross outer = as(map.combinator(), MapCombinator.Cross.class);
        MapCombinator.Cross inner = as(outer.left(), MapCombinator.Cross.class);
        assertEquals("a", as(inner.left(), MapCombinator.Leaf.class).name());
        assertEquals("b", as(inner.right(), MapCombinator.Leaf.class).name());
        assertEquals("c", as(outer.right(), MapCombinator.Leaf.class).name());
    }

    public void testParenthesesOverridePrecedence() {
        // (a ZIP b) CROSS c must parse as Cross(Zip(a, b), c).
        MapCommand map = parseMap("MAP (a ZIP b) CROSS c RETURNING x [ EVAL x = a ]");
        MapCombinator.Cross cross = as(map.combinator(), MapCombinator.Cross.class);
        MapCombinator.Zip zip = as(cross.left(), MapCombinator.Zip.class);
        assertEquals("a", as(zip.left(), MapCombinator.Leaf.class).name());
        assertEquals("b", as(zip.right(), MapCombinator.Leaf.class).name());
        assertEquals("c", as(cross.right(), MapCombinator.Leaf.class).name());
    }

    public void testLeavesOrder() {
        MapCommand map = parseMap("MAP a CROSS b ZIP c RETURNING x [ EVAL x = a ]");
        List<String> names = map.combinator().leaves().stream().map(MapCombinator.Leaf::name).toList();
        assertThat(names, contains("a", "b", "c"));
    }

    public void testMultiCommandSubPipeline() {
        MapCommand map = parseMap("MAP a RETURNING x [ EVAL y = a | EVAL x = y ]");
        Eval outer = as(map.subPipeline(), Eval.class);
        assertThat(outer.child(), instanceOf(Eval.class));
    }

    public void testMissingReturningFails() {
        expectError("row a = 1 | MAP a [ EVAL x = a ]", "missing 'returning'");
    }

    public void testMissingSubPipelineFails() {
        expectError("row a = 1 | MAP a RETURNING x", "mismatched input");
    }
}
