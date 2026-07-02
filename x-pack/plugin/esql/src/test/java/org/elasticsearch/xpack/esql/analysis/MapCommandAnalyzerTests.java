/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.compute.operator.MapCombinator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MapCommand;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;

/**
 * Analyzer-level tests for the MAP command.
 * <p>
 *     These exercise {@code ResolveRefs#resolveMapCommand}: leaf channels are resolved against
 *     the child output, a synthetic {@code _map_col_<name>} relation feeds the sub-pipeline, and
 *     the RETURNING attribute is resolved against the sub-pipeline output. The verifier checks in
 *     {@code Verifier#checkMapCommand} are covered by the error cases.
 * </p>
 */
public class MapCommandAnalyzerTests extends ESTestCase {

    private static final String NO_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";

    private static TestAnalyzer defaultAnalyzer() {
        return analyzer().addDefaultIndex().stripErrorPrefix(true);
    }

    private MapCommand analyzeMap(String query) {
        LogicalPlan plan = defaultAnalyzer().query(query);
        return plan.collect(MapCommand.class).stream().findFirst().orElseThrow();
    }

    public void testResolvesLeafChannel() {
        MapCommand map = analyzeMap("FROM test | MAP salary RETURNING x [ EVAL x = salary ]");
        MapCombinator.Leaf leaf = as(map.combinator(), MapCombinator.Leaf.class);
        assertEquals("salary", leaf.name());
        // The analyzer replaces the parser's -1 placeholder with the real child channel.
        assertTrue("leaf channel should be resolved, was " + leaf.channel(), leaf.channel() >= 0);
        assertWarnings(NO_LIMIT_WARNING);
    }

    public void testResolvesReturningAttribute() {
        MapCommand map = analyzeMap("FROM test | MAP salary RETURNING x [ EVAL x = salary + 1 ]");
        Attribute returning = map.returningAttr();
        assertEquals("x", returning.name());
        assertTrue("RETURNING attribute should be resolved", returning.resolved());
        assertWarnings(NO_LIMIT_WARNING);
    }

    public void testAddsReturningColumnToOutput() {
        MapCommand map = analyzeMap("FROM test | MAP salary RETURNING x [ EVAL x = salary ]");
        assertTrue(map.output().stream().anyMatch(a -> a.name().equals("x")));
        assertWarnings(NO_LIMIT_WARNING);
    }

    public void testCrossCombinatorResolvesAllLeaves() {
        MapCommand map = analyzeMap("FROM test | MAP salary CROSS emp_no RETURNING x [ EVAL x = salary ]");
        MapCombinator.Cross cross = as(map.combinator(), MapCombinator.Cross.class);
        assertTrue(as(cross.left(), MapCombinator.Leaf.class).channel() >= 0);
        assertTrue(as(cross.right(), MapCombinator.Leaf.class).channel() >= 0);
        assertWarnings(NO_LIMIT_WARNING);
    }

    public void testUnknownLeafColumnFails() {
        defaultAnalyzer().error(
            "FROM test | MAP does_not_exist RETURNING x [ EVAL x = does_not_exist ]",
            containsString("MAP combinator references unknown column [does_not_exist]")
        );
        assertWarnings(NO_LIMIT_WARNING);
    }

    /**
     * The RETURNING attribute is resolved against the sub-pipeline output; when it names a column
     * the sub-pipeline never produces, attribute resolution reports it as an unknown column before
     * the verifier's dedicated MAP check runs.
     */
    public void testReturningColumnNotProducedFails() {
        defaultAnalyzer().error("FROM test | MAP salary RETURNING missing [ EVAL x = salary ]", containsString("Unknown column [missing]"));
        assertWarnings(NO_LIMIT_WARNING);
    }
}
