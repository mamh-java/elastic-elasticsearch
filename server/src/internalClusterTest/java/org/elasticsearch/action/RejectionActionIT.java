/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.lang.management.ManagementFactory;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class RejectionActionIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("thread_pool.search.size", 1)
            .put("thread_pool.search.queue_size", 1)
            .put("thread_pool.get.size", 1)
            .put("thread_pool.get.queue_size", 1)
            .build();
    }

    public void testSimulatedSearchRejectionLoad() throws Throwable {
        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "1").get();
        }

        int numberOfAsyncOps = randomIntBetween(200, 700);
        final CountDownLatch latch = new CountDownLatch(numberOfAsyncOps);
        final CopyOnWriteArrayList<Object> responses = new CopyOnWriteArrayList<>();
        for (int i = 0; i < numberOfAsyncOps; i++) {
            prepareSearch("test").setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("field", "1"))
                .execute(new LatchedActionListener<>(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        responses.add(searchResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        responses.add(e);
                    }
                }, latch));
        }
        // Bounded wait: some search requests can be stuck indefinitely (see #146022), in which case we'd
        // rather fail fast with a clear timeout than have the whole suite hang until it hits its own timeout.
        if (latch.await(1, TimeUnit.MINUTES) == false) {
            dumpStuckSearchDiagnostics(numberOfAsyncOps, responses.size());
            fail("timed out: " + (numberOfAsyncOps - responses.size()) + " of " + numberOfAsyncOps + " searches never completed");
        }

        // validate all responses
        for (Object response : responses) {
            if (response instanceof SearchResponse searchResponse) {
                for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                    assertThat(
                        failure.reason().toLowerCase(Locale.ENGLISH),
                        anyOf(containsString("cancelled"), containsString("rejected"))
                    );
                }
            } else {
                Exception t = (Exception) response;
                Throwable unwrap = ExceptionsHelper.unwrapCause(t);
                if (unwrap instanceof SearchPhaseExecutionException e) {
                    for (ShardSearchFailure failure : e.shardFailures()) {
                        assertThat(
                            failure.reason().toLowerCase(Locale.ENGLISH),
                            anyOf(containsString("cancelled"), containsString("rejected"))
                        );
                    }
                } else if ((unwrap instanceof EsRejectedExecutionException) == false) {
                    throw new AssertionError("unexpected failure", (Throwable) response);
                }
            }
        }
        assertThat(responses.size(), equalTo(numberOfAsyncOps));
    }

    private void dumpStuckSearchDiagnostics(int numberOfAsyncOps, int completed) {
        StringBuilder sb = new StringBuilder("RejectionActionIT stuck: ").append(numberOfAsyncOps - completed)
            .append(" of ")
            .append(numberOfAsyncOps)
            .append(" searches never invoked their listener\n");

        var threadBean = ManagementFactory.getThreadMXBean();
        sb.append("thread dump:\n");
        for (var info : threadBean.dumpAllThreads(true, true)) {
            sb.append('"').append(info.getThreadName()).append("\" ").append(info.getThreadState());
            if (info.getLockInfo() != null) {
                sb.append(" waiting on ").append(info.getLockInfo());
                if (info.getLockOwnerName() != null) {
                    sb.append(" held by \"").append(info.getLockOwnerName()).append('"');
                }
            }
            sb.append('\n');
            for (var frame : info.getStackTrace()) {
                sb.append("\tat ").append(frame).append('\n');
            }
            sb.append('\n');
        }

        sb.append("running search tasks:\n");
        try {
            var tasks = clusterAdmin().prepareListTasks()
                .setActions("indices:data/read/search*")
                .setDetailed(true)
                .get(TimeValue.timeValueSeconds(10))
                .getTasks();
            sb.append("count=").append(tasks.size()).append('\n');
            for (var task : tasks) {
                sb.append("  ").append(task).append('\n');
            }
        } catch (Exception e) {
            sb.append("failed to list running search tasks: ").append(e).append('\n');
        }

        logger.error("{}", sb);
    }
}
