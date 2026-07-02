/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end integration tests for the snapshot-only {@code MAP} command.
 * <p>
 *     Each test indexes real documents with multi-value integer fields, runs a {@code MAP}
 *     query through the ESQL action, and asserts the shape and values of the {@code RETURNING}
 *     multi-value column. The mini-table combinators are exercised directly:
 *     {@code CROSS} (cartesian product), {@code ZIP} (positional pairing with null-pad), and the
 *     fully-filtered-position case where {@code RETURNING} collapses to {@code null}.
 * </p>
 * <p>
 *     The command is gated behind the {@code MAP_COMMAND} capability, which is only enabled on
 *     snapshot builds; the whole suite is skipped otherwise via the {@code assumeTrue} guard in
 *     {@link #setup()}.
 * </p>
 */
public class MapCommandIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setup() {
        assumeTrue("MAP command is only enabled on snapshot builds", EsqlCapabilities.Cap.MAP_COMMAND.isEnabled());
        assertAcked(
            client().admin().indices().prepareCreate("mvdocs").setMapping("k", "type=keyword", "a", "type=integer", "b", "type=integer")
        );
        // One document per shape, keyed by the single-valued field "k" so each test can select
        // exactly one document; the multi-value fields "a"/"b" then make RETURNING deterministic.
        client().prepareBulk()
            .add(new IndexRequest("mvdocs").id("equal").source("k", "equal", "a", List.of(1, 2), "b", List.of(10, 20)))
            .add(new IndexRequest("mvdocs").id("longerA").source("k", "longerA", "a", List.of(1, 2, 3), "b", List.of(10, 20)))
            .add(new IndexRequest("mvdocs").id("single").source("k", "single", "a", List.of(1, 2, 3), "b", 10))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    public void testCrossEval() {
        try (EsqlQueryResponse resp = run("FROM mvdocs | WHERE k == \"equal\" | MAP a CROSS b RETURNING s [ EVAL s = a + b ] | KEEP s")) {
            assertThat(resp.columns().get(0).name(), equalTo("s"));
            assertThat(resp.columns().get(0).type(), equalTo(DataType.INTEGER));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            // Cross iterates left-outer/right-inner: (1,10),(1,20),(2,10),(2,20) -> a + b.
            assertThat(values.get(0).get(0), equalTo(List.of(11, 21, 12, 22)));
        }
    }

    public void testZipEqualLength() {
        try (EsqlQueryResponse resp = run("FROM mvdocs | WHERE k == \"equal\" | MAP a ZIP b RETURNING s [ EVAL s = a + b ] | KEEP s")) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            // Positional pairing: (1,10),(2,20).
            assertThat(values.get(0).get(0), equalTo(List.of(11, 22)));
        }
    }

    public void testZipMismatchedLengths() {
        // a=[1,2,3] ZIP b=[10,20] -> (1,10),(2,20),(3,null); the null-padded row's RETURNING is
        // null and is dropped from the multi-value output, leaving two values.
        try (EsqlQueryResponse resp = run("FROM mvdocs | WHERE k == \"longerA\" | MAP a ZIP b RETURNING s [ EVAL s = a + b ] | KEEP s")) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0).get(0), equalTo(List.of(11, 22)));
        }
    }

    public void testZipMismatchedLengthsRawColumn() {
        // RETURNING the raw padded column keeps only its non-null positions: b = [10, 20].
        try (EsqlQueryResponse resp = run("FROM mvdocs | WHERE k == \"longerA\" | MAP a ZIP b RETURNING s [ EVAL s = b ] | KEEP s")) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0).get(0), equalTo(List.of(10, 20)));
        }
    }

    public void testWhereEliminatesAllRows() {
        // WHERE removes every mini-table row for the document, so RETURNING collapses to null.
        try (
            EsqlQueryResponse resp = run(
                "FROM mvdocs | WHERE k == \"equal\" | MAP a CROSS b RETURNING s [ EVAL s = a + b | WHERE s > 1000 ] | KEEP s"
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0).get(0), nullValue());
        }
    }

    public void testCrossWithSingleValuedColumn() {
        // b is single-valued (10); CROSS with it just adds 10 to every a value.
        try (EsqlQueryResponse resp = run("FROM mvdocs | WHERE k == \"single\" | MAP a CROSS b RETURNING s [ EVAL s = a + b ] | KEEP s")) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0).get(0), equalTo(List.of(11, 12, 13)));
        }
    }

    public void testDisallowedSubPipelineCommandFails() {
        // SORT is a pipeline breaker; the RETURNING column is still produced by the EVAL, so the
        // verifier reaches its dedicated MAP sub-pipeline check rather than an unknown-column error.
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> run("FROM mvdocs | MAP a RETURNING s [ EVAL s = a | SORT s ]")
        );
        assertThat(e.getMessage(), containsString("MAP sub-pipeline cannot contain pipeline-breaking commands"));
    }

    public void testReturningColumnNotProducedFails() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> run("FROM mvdocs | MAP a RETURNING missing [ EVAL s = a ]")
        );
        assertThat(e.getMessage(), containsString("Unknown column [missing]"));
    }
}
