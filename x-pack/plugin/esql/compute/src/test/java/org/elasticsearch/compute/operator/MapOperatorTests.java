/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Unit tests for {@link MapExpandOperator} and {@link MapContractOperator}.
 * <p>
 *     Operators are wired directly as Java objects without a Driver.  For each test
 *     the pipeline is:
 * </p>
 * <pre>
 *   input page → MapExpandOperator → [optional EvalOperator/FilterOperator] → MapContractOperator
 * </pre>
 */
public class MapOperatorTests extends ComputeTestCase {

    /**
     * Builds a {@link DriverContext} backed by the test's non-breaking {@link BlockFactory}.
     */
    private DriverContext driverContext() {
        BlockFactory factory = blockFactory();
        return new DriverContext(factory.bigArrays(), factory, null);
    }

    /**
     * Builds an MV int block for a single position with the given values.
     * If {@code values} is empty the position is null.
     */
    private static Block mvIntBlock(BlockFactory factory, int... values) {
        IntBlock.Builder b = factory.newIntBlockBuilder(1);
        if (values.length == 0) {
            b.appendNull();
        } else if (values.length == 1) {
            b.appendInt(values[0]);
        } else {
            b.beginPositionEntry();
            for (int v : values) {
                b.appendInt(v);
            }
            b.endPositionEntry();
        }
        return b.build();
    }

    /**
     * Drains all output from an operator that has been given all its input (and
     * optionally had {@link Operator#finish()} called).
     */
    private static List<Page> drainOutput(Operator op) {
        List<Page> result = new ArrayList<>();
        while (op.canProduceMoreDataWithoutExtraInput()) {
            Page p = op.getOutput();
            if (p != null) {
                result.add(p);
            }
        }
        Page p = op.getOutput();
        if (p != null) {
            result.add(p);
        }
        return result;
    }

    /**
     * Passes all pages from {@code inputPages} through {@code op}, draining output
     * after each {@link Operator#addInput} call, then calls {@link Operator#finish()} and
     * drains any remaining output.
     */
    private static List<Page> runOperator(Operator op, List<Page> inputPages) {
        List<Page> output = new ArrayList<>();
        for (Page p : inputPages) {
            op.addInput(p);
            output.addAll(drainOutput(op));
        }
        op.finish();
        output.addAll(drainOutput(op));
        return output;
    }

    /**
     * Runs the full MAP pipeline (expand → optional middle operators → contract) and
     * returns the contract output pages.
     * <p>
     *     NOTE: Each operator is responsible for releasing its own input pages.  The
     *     FilterOperator and EvalOperator (via AbstractPageMappingOperator) handle this
     *     internally.  MapContractOperator releases pages in its {@code addInput} method.
     *     Therefore this method does NOT perform any extra page-release calls.
     * </p>
     *
     * @param expandOp         the expand operator
     * @param middleOps        zero or more operators between expand and contract (e.g. eval, filter)
     * @param contractOp       the contract operator
     * @param inputPages       source pages
     */
    private static List<Page> runPipeline(
        MapExpandOperator expandOp,
        List<Operator> middleOps,
        MapContractOperator contractOp,
        List<Page> inputPages
    ) {
        List<Page> current = runOperator(expandOp, inputPages);

        for (Operator op : middleOps) {
            current = runOperator(op, current);
        }

        return runOperator(contractOp, current);
    }

    /**
     * Collects all int values from the RETURNING column (last channel) of the output pages
     * into a flat list.  Each position may be null (adds null to the list) or MV (adds each
     * value separately).
     */
    private static List<Integer> collectReturning(List<Page> pages) {
        List<Integer> result = new ArrayList<>();
        for (Page page : pages) {
            IntBlock ret = page.getBlock(page.getBlockCount() - 1);
            for (int p = 0; p < ret.getPositionCount(); p++) {
                int count = ret.getValueCount(p);
                if (count == 0) {
                    result.add(null);
                } else {
                    int first = ret.getFirstValueIndex(p);
                    for (int i = 0; i < count; i++) {
                        result.add(ret.getInt(first + i));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Evaluator that adds the int values at {@code lhsChannel} and {@code rhsChannel} and
     * appends the result as a new channel.
     */
    record SumTwoChannels(DriverContext ctx, int lhs, int rhs) implements ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            IntBlock lhsBlock = page.getBlock(lhs);
            IntBlock rhsBlock = page.getBlock(rhs);
            try (IntVector.FixedBuilder builder = ctx.blockFactory().newIntVectorFixedBuilder(page.getPositionCount())) {
                for (int p = 0; p < page.getPositionCount(); p++) {
                    int lhsVal = lhsBlock.isNull(p) ? 0 : lhsBlock.getInt(lhsBlock.getFirstValueIndex(p));
                    int rhsVal = rhsBlock.isNull(p) ? 0 : rhsBlock.getInt(rhsBlock.getFirstValueIndex(p));
                    builder.appendInt(lhsVal + rhsVal);
                }
                return builder.build().asBlock();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {}
    }

    /**
     * Evaluator that adds THREE int channel values.
     */
    record SumThreeChannels(DriverContext ctx, int ch0, int ch1, int ch2) implements ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            IntBlock b0 = page.getBlock(ch0);
            IntBlock b1 = page.getBlock(ch1);
            IntBlock b2 = page.getBlock(ch2);
            try (IntVector.FixedBuilder builder = ctx.blockFactory().newIntVectorFixedBuilder(page.getPositionCount())) {
                for (int p = 0; p < page.getPositionCount(); p++) {
                    int v0 = b0.isNull(p) ? 0 : b0.getInt(b0.getFirstValueIndex(p));
                    int v1 = b1.isNull(p) ? 0 : b1.getInt(b1.getFirstValueIndex(p));
                    int v2 = b2.isNull(p) ? 0 : b2.getInt(b2.getFirstValueIndex(p));
                    builder.appendInt(v0 + v1 + v2);
                }
                return builder.build().asBlock();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {}
    }

    /**
     * Evaluator that loads a channel as the result (pass-through, increments ref count).
     */
    record LoadChannel(int channel) implements ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            Block b = page.getBlock(channel);
            b.incRef();
            return b;
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {}
    }

    /**
     * Filter evaluator: keeps rows where {@code channel} is non-null AND its single int
     * value is {@code >= threshold}.
     */
    record KeepIfIntGte(DriverContext ctx, int channel, int threshold) implements ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            IntBlock block = page.getBlock(channel);
            try (BooleanVector.FixedBuilder builder = ctx.blockFactory().newBooleanVectorFixedBuilder(page.getPositionCount())) {
                for (int p = 0; p < page.getPositionCount(); p++) {
                    if (block.isNull(p)) {
                        builder.appendBoolean(false);
                    } else {
                        builder.appendBoolean(block.getInt(block.getFirstValueIndex(p)) >= threshold);
                    }
                }
                return builder.build().asBlock();
            }
        }

        @Override
        public long baseRamBytesUsed() {
            return 0;
        }

        @Override
        public void close() {}
    }

    /**
     * Two MV int columns {@code a=[1,2]}, {@code b=[10,20]}.
     * Combinator = {@code Cross(Leaf(a), Leaf(b))}.
     * RETURNING = {@code a + b}.
     * Verify 4 RETURNING values: 11, 21, 12, 22.
     */
    public void testFlatCrossReturningCount() {
        DriverContext ctx = driverContext();
        BlockFactory factory = ctx.blockFactory();

        // Channel layout after expand:
        // 0 = original a (broadcast)
        // 1 = original b (broadcast)
        // 2 = _map_col_a (scalar)
        // 3 = _map_col_b (scalar)
        // 4 = _map_pos
        // 5 = _map_page_id

        MapCombinator combinator = new MapCombinator.Cross(new MapCombinator.Leaf(0, "a"), new MapCombinator.Leaf(1, "b"));
        int[] leafChannels = { 0, 1 };
        String[] leafNames = { "a", "b" };
        int mapPosChannel = 4;
        int mapPageIdChannel = 5;
        MapPageTracker tracker = new MapPageTracker(ctx.blockFactory().breaker());

        MapExpandOperator expand = new MapExpandOperator(combinator, leafChannels, leafNames, mapPosChannel, 100, ctx, tracker);

        // EvalOperator: appends sum of _map_col_a (ch 2) and _map_col_b (ch 3) at ch 6
        EvalOperator eval = new EvalOperator(ctx, new SumTwoChannels(ctx, 2, 3));

        // Contract: source = channels [0, 1], returning = ch 6, map_pos = ch 4, page_id = ch 5
        MapContractOperator contract = new MapContractOperator(
            mapPosChannel,
            mapPageIdChannel,
            6,
            new int[] { 2, 3 },
            new int[] { 0, 1 },
            0,
            ctx,
            tracker
        );

        // Input: one document with a=[1,2], b=[10,20]
        Block aBlock = mvIntBlock(factory, 1, 2);
        Block bBlock = mvIntBlock(factory, 10, 20);
        Page input = new Page(aBlock, bBlock);

        List<Page> output;
        try (tracker; expand; eval; contract) {
            output = runPipeline(expand, List.of(eval), contract, List.of(input));
        }

        // Expect 1 output page with 1 position and 4 RETURNING values
        assertThat(output, hasSize(1));
        Page out = output.get(0);
        assertThat(out.getPositionCount(), equalTo(1));

        // RETURNING is MV at position 0 with values 11, 21, 12, 22 (in cross order)
        IntBlock ret = out.getBlock(out.getBlockCount() - 1);
        assertThat(ret.getValueCount(0), equalTo(4));
        List<Integer> retValues = new ArrayList<>();
        int first = ret.getFirstValueIndex(0);
        for (int i = 0; i < 4; i++) {
            retValues.add(ret.getInt(first + i));
        }
        assertThat(retValues, equalTo(Arrays.asList(11, 21, 12, 22)));

        for (Page p : output) {
            p.releaseBlocks();
        }
    }

    /**
     * {@code a=[1,2]}, {@code b=[10,20]}.
     * Combinator = {@code Zip(Leaf(a), Leaf(b))}.
     * RETURNING = {@code a + b}.
     * Expect 2 values: [11, 22].
     */
    public void testFlatZipEqualLength() {
        DriverContext ctx = driverContext();
        BlockFactory factory = ctx.blockFactory();

        MapCombinator combinator = new MapCombinator.Zip(new MapCombinator.Leaf(0, "a"), new MapCombinator.Leaf(1, "b"));
        int[] leafChannels = { 0, 1 };
        String[] leafNames = { "a", "b" };
        int mapPosChannel = 4;
        int mapPageIdChannel = 5;
        MapPageTracker tracker = new MapPageTracker(ctx.blockFactory().breaker());

        MapExpandOperator expand = new MapExpandOperator(combinator, leafChannels, leafNames, mapPosChannel, 100, ctx, tracker);
        EvalOperator eval = new EvalOperator(ctx, new SumTwoChannels(ctx, 2, 3));
        MapContractOperator contract = new MapContractOperator(
            mapPosChannel,
            mapPageIdChannel,
            6,
            new int[] { 2, 3 },
            new int[] { 0, 1 },
            0,
            ctx,
            tracker
        );

        Block aBlock = mvIntBlock(factory, 1, 2);
        Block bBlock = mvIntBlock(factory, 10, 20);
        Page input = new Page(aBlock, bBlock);

        List<Page> output;
        try (tracker; expand; eval; contract) {
            output = runPipeline(expand, List.of(eval), contract, List.of(input));
        }

        List<Integer> ret = collectReturning(output);
        assertThat(ret, equalTo(Arrays.asList(11, 22)));
        for (Page p : output) {
            p.releaseBlocks();
        }
    }

    /**
     * {@code a=[1,2,3]}, {@code b=[10,20]}.
     * Combinator = {@code Zip(Leaf(a), Leaf(b))}.
     * RETURNING = raw {@code _map_col_b}.
     * Expect 3 values: [10, 20, null].
     */
    public void testZipMismatchedLengths() {
        DriverContext ctx = driverContext();
        BlockFactory factory = ctx.blockFactory();

        // Channel layout after expand:
        // 0 = original a (broadcast)
        // 1 = original b (broadcast)
        // 2 = _map_col_a (scalar)
        // 3 = _map_col_b (scalar)
        // 4 = _map_pos
        // 5 = _map_page_id

        MapCombinator combinator = new MapCombinator.Zip(new MapCombinator.Leaf(0, "a"), new MapCombinator.Leaf(1, "b"));
        int[] leafChannels = { 0, 1 };
        String[] leafNames = { "a", "b" };
        int mapPosChannel = 4;
        int mapPageIdChannel = 5;
        MapPageTracker tracker = new MapPageTracker(ctx.blockFactory().breaker());

        MapExpandOperator expand = new MapExpandOperator(combinator, leafChannels, leafNames, mapPosChannel, 100, ctx, tracker);

        // RETURNING = _map_col_b (ch 3), pass through as-is; appended at ch 6
        EvalOperator eval = new EvalOperator(ctx, new LoadChannel(3));

        MapContractOperator contract = new MapContractOperator(
            mapPosChannel,
            mapPageIdChannel,
            6,
            new int[] { 2, 3 },
            new int[] { 0, 1 },
            0,
            ctx,
            tracker
        );

        Block aBlock = mvIntBlock(factory, 1, 2, 3);
        Block bBlock = mvIntBlock(factory, 10, 20);
        Page input = new Page(aBlock, bBlock);

        List<Page> output;
        try (tracker; expand; eval; contract) {
            output = runPipeline(expand, List.of(eval), contract, List.of(input));
        }

        assertThat(output, hasSize(1));
        Page out = output.get(0);
        assertThat(out.getPositionCount(), equalTo(1));

        IntBlock ret = out.getBlock(out.getBlockCount() - 1);
        // 3 values: 10, 20, null → null-padded zip row means we skip it in MV
        // The third zip row has _map_col_b = null, so RETURNING is null for that row.
        // After filter: 2 non-null values → MV [10, 20]
        // But NO filter is applied here — all 3 rows survive, so:
        // - Row 0: returning = 10 (non-null)
        // - Row 1: returning = 20 (non-null)
        // - Row 2: returning = null (_map_col_b was null → LoadChannel(3) returns null)
        // MergeReturningValues: 2 non-null → MV [10, 20]
        assertThat(ret.getValueCount(0), equalTo(2));
        int first = ret.getFirstValueIndex(0);
        assertThat(ret.getInt(first), equalTo(10));
        assertThat(ret.getInt(first + 1), equalTo(20));

        for (Page p : output) {
            p.releaseBlocks();
        }
    }

    /**
     * Same as {@link #testZipMismatchedLengths} but with a FilterOperator that keeps
     * only non-null {@code _map_col_b} rows.
     * Expect RETURNING = [10, 20] (MV with 2 values).
     */
    public void testZipFilterNonNull() {
        DriverContext ctx = driverContext();
        BlockFactory factory = ctx.blockFactory();

        MapCombinator combinator = new MapCombinator.Zip(new MapCombinator.Leaf(0, "a"), new MapCombinator.Leaf(1, "b"));
        int[] leafChannels = { 0, 1 };
        String[] leafNames = { "a", "b" };
        int mapPosChannel = 4;
        int mapPageIdChannel = 5;
        MapPageTracker tracker = new MapPageTracker(ctx.blockFactory().breaker());

        MapExpandOperator expand = new MapExpandOperator(combinator, leafChannels, leafNames, mapPosChannel, 100, ctx, tracker);

        // Filter: keep rows where _map_col_b (ch 3) is non-null
        FilterOperator filter = new FilterOperator(new ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                IntBlock b = page.getBlock(3);
                BooleanVector.FixedBuilder builder = ctx.blockFactory().newBooleanVectorFixedBuilder(page.getPositionCount());
                for (int p = 0; p < page.getPositionCount(); p++) {
                    builder.appendBoolean(b.isNull(p) == false);
                }
                return builder.build().asBlock();
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        });

        // RETURNING = _map_col_b (ch 3), appended at ch 6
        EvalOperator eval = new EvalOperator(ctx, new LoadChannel(3));

        MapContractOperator contract = new MapContractOperator(
            mapPosChannel,
            mapPageIdChannel,
            6,
            new int[] { 2, 3 },
            new int[] { 0, 1 },
            0,
            ctx,
            tracker
        );

        Block aBlock = mvIntBlock(factory, 1, 2, 3);
        Block bBlock = mvIntBlock(factory, 10, 20);
        Page input = new Page(aBlock, bBlock);

        List<Page> output;
        try (tracker; expand; filter; eval; contract) {
            output = runPipeline(expand, List.of(filter, eval), contract, List.of(input));
        }

        assertThat(output, hasSize(1));
        Page out = output.get(0);
        assertThat(out.getPositionCount(), equalTo(1));

        IntBlock ret = out.getBlock(out.getBlockCount() - 1);
        assertThat(ret.getValueCount(0), equalTo(2));
        int first = ret.getFirstValueIndex(0);
        assertThat(ret.getInt(first), equalTo(10));
        assertThat(ret.getInt(first + 1), equalTo(20));

        for (Page p : output) {
            p.releaseBlocks();
        }
    }

    /**
     * {@code a=[1,2]}, {@code b=[10,20]}, {@code c=[100,200]}.
     * Combinator = {@code Cross(Leaf(a), Zip(Leaf(b), Leaf(c)))}.
     * RETURNING = {@code a + b + c}.
     * Expect 4 values: [111, 221, 112, 222].
     * <p>
     *     Cross a with Zip(b,c): a=1 cross (b=10,c=100), a=1 cross (b=20,c=200),
     *     a=2 cross (b=10,c=100), a=2 cross (b=20,c=200).
     * </p>
     */
    public void testTreeCombinatorCrossZip() {
        DriverContext ctx = driverContext();
        BlockFactory factory = ctx.blockFactory();

        // Channel layout after expand (inputs: ch0=a, ch1=b, ch2=c):
        // 0 = original a (broadcast)
        // 1 = original b (broadcast)
        // 2 = original c (broadcast)
        // 3 = _map_col_a (scalar) — leaf 0
        // 4 = _map_col_b (scalar) — leaf 1
        // 5 = _map_col_c (scalar) — leaf 2
        // 6 = _map_pos
        // 7 = _map_page_id

        MapCombinator combinator = new MapCombinator.Cross(
            new MapCombinator.Leaf(0, "a"),
            new MapCombinator.Zip(new MapCombinator.Leaf(1, "b"), new MapCombinator.Leaf(2, "c"))
        );
        int[] leafChannels = { 0, 1, 2 };
        String[] leafNames = { "a", "b", "c" };
        int mapPosChannel = 6;
        int mapPageIdChannel = 7;
        MapPageTracker tracker = new MapPageTracker(ctx.blockFactory().breaker());

        MapExpandOperator expand = new MapExpandOperator(combinator, leafChannels, leafNames, mapPosChannel, 100, ctx, tracker);

        // RETURNING = _map_col_a (ch 3) + _map_col_b (ch 4) + _map_col_c (ch 5), at ch 8
        EvalOperator eval = new EvalOperator(ctx, new SumThreeChannels(ctx, 3, 4, 5));

        MapContractOperator contract = new MapContractOperator(
            mapPosChannel,
            mapPageIdChannel,
            8,
            new int[] { 3, 4, 5 },
            new int[] { 0, 1, 2 },
            0,
            ctx,
            tracker
        );

        Block aBlock = mvIntBlock(factory, 1, 2);
        Block bBlock = mvIntBlock(factory, 10, 20);
        Block cBlock = mvIntBlock(factory, 100, 200);
        Page input = new Page(aBlock, bBlock, cBlock);

        List<Page> output;
        try (tracker; expand; eval; contract) {
            output = runPipeline(expand, List.of(eval), contract, List.of(input));
        }

        assertThat(output, hasSize(1));
        Page out = output.get(0);
        assertThat(out.getPositionCount(), equalTo(1));

        IntBlock ret = out.getBlock(out.getBlockCount() - 1);
        assertThat(ret.getValueCount(0), equalTo(4));
        int first = ret.getFirstValueIndex(0);
        assertThat(ret.getInt(first), equalTo(111));
        assertThat(ret.getInt(first + 1), equalTo(221));
        assertThat(ret.getInt(first + 2), equalTo(112));
        assertThat(ret.getInt(first + 3), equalTo(222));

        for (Page p : output) {
            p.releaseBlocks();
        }
    }

    /**
     * {@code a=[1,2,3]}, {@code b=[10]} (single value).
     * Combinator = {@code Cross(Leaf(a), Leaf(b))}.
     * RETURNING = {@code a + b}.
     * Expect 3 values: [11, 12, 13].
     */
    public void testCrossWithSingleElementColumn() {
        DriverContext ctx = driverContext();
        BlockFactory factory = ctx.blockFactory();

        MapCombinator combinator = new MapCombinator.Cross(new MapCombinator.Leaf(0, "a"), new MapCombinator.Leaf(1, "b"));
        int[] leafChannels = { 0, 1 };
        String[] leafNames = { "a", "b" };
        int mapPosChannel = 4;
        int mapPageIdChannel = 5;
        MapPageTracker tracker = new MapPageTracker(ctx.blockFactory().breaker());

        MapExpandOperator expand = new MapExpandOperator(combinator, leafChannels, leafNames, mapPosChannel, 100, ctx, tracker);
        EvalOperator eval = new EvalOperator(ctx, new SumTwoChannels(ctx, 2, 3));
        MapContractOperator contract = new MapContractOperator(
            mapPosChannel,
            mapPageIdChannel,
            6,
            new int[] { 2, 3 },
            new int[] { 0, 1 },
            0,
            ctx,
            tracker
        );

        Block aBlock = mvIntBlock(factory, 1, 2, 3);
        Block bBlock = mvIntBlock(factory, 10);
        Page input = new Page(aBlock, bBlock);

        List<Page> output;
        try (tracker; expand; eval; contract) {
            output = runPipeline(expand, List.of(eval), contract, List.of(input));
        }

        List<Integer> ret = collectReturning(output);
        assertThat(ret, equalTo(Arrays.asList(11, 12, 13)));
        for (Page p : output) {
            p.releaseBlocks();
        }
    }

    /**
     * Two documents: doc0 {@code a=[1,2]}, doc1 {@code a=[5,6]}.
     * FilterOperator blocks all rows where {@code _map_col_a < 5}.
     * Expect: doc0's RETURNING is null, doc1's RETURNING has 2 values [5, 6].
     */
    public void testWhereFiltersAllRowsForPosition() {
        DriverContext ctx = driverContext();
        BlockFactory factory = ctx.blockFactory();

        // Channel layout after expand (1 input col a at ch 0):
        // 0 = original a (broadcast)
        // 1 = _map_col_a (scalar)
        // 2 = _map_pos

        MapCombinator combinator = new MapCombinator.Leaf(0, "a");
        int[] leafChannels = { 0 };
        String[] leafNames = { "a" };
        int mapPosChannel = 2;
        int mapPageIdChannel = 3;
        MapPageTracker tracker = new MapPageTracker(ctx.blockFactory().breaker());

        MapExpandOperator expand = new MapExpandOperator(combinator, leafChannels, leafNames, mapPosChannel, 100, ctx, tracker);

        // Filter: keep rows where _map_col_a (ch 1) >= 5
        FilterOperator filter = new FilterOperator(new KeepIfIntGte(ctx, 1, 5));

        // RETURNING = _map_col_a (ch 1), appended as ch 4
        EvalOperator eval = new EvalOperator(ctx, new LoadChannel(1));

        MapContractOperator contract = new MapContractOperator(
            mapPosChannel,
            mapPageIdChannel,
            4,
            new int[] { 1 },
            new int[] { 0 },
            0,
            ctx,
            tracker
        );

        // Two documents in one page: a=[1,2] at pos 0, a=[5,6] at pos 1
        IntBlock.Builder aBuilder = factory.newIntBlockBuilder(2);
        aBuilder.beginPositionEntry();
        aBuilder.appendInt(1);
        aBuilder.appendInt(2);
        aBuilder.endPositionEntry();
        aBuilder.beginPositionEntry();
        aBuilder.appendInt(5);
        aBuilder.appendInt(6);
        aBuilder.endPositionEntry();
        Block aBlock = aBuilder.build();
        Page input = new Page(aBlock);

        List<Page> output;
        try (tracker; expand; filter; eval; contract) {
            output = runPipeline(expand, List.of(filter, eval), contract, List.of(input));
        }

        assertThat(output, hasSize(1));
        Page out = output.get(0);
        assertThat(out.getPositionCount(), equalTo(2));

        IntBlock ret = out.getBlock(out.getBlockCount() - 1);
        // doc0: all rows filtered out → null
        assertThat(ret.isNull(0), equalTo(true));
        // doc1: 2 values [5, 6]
        assertThat(ret.getValueCount(1), equalTo(2));
        int first = ret.getFirstValueIndex(1);
        assertThat(ret.getInt(first), equalTo(5));
        assertThat(ret.getInt(first + 1), equalTo(6));

        for (Page p : output) {
            p.releaseBlocks();
        }
    }

    /**
     * Two input pages, each with one document: page1 {@code a=[1,2]}, page2 {@code a=[3,4]}.
     * Verify that {@code _map_pos} resets to 0 for the second page and that both pages
     * produce correct RETURNING results.
     */
    public void testMultipleInputPages() {
        DriverContext ctx = driverContext();
        BlockFactory factory = ctx.blockFactory();

        MapCombinator combinator = new MapCombinator.Leaf(0, "a");
        int[] leafChannels = { 0 };
        String[] leafNames = { "a" };
        int mapPosChannel = 2;
        int mapPageIdChannel = 3;
        MapPageTracker tracker = new MapPageTracker(ctx.blockFactory().breaker());

        MapExpandOperator expand = new MapExpandOperator(combinator, leafChannels, leafNames, mapPosChannel, 100, ctx, tracker);

        // RETURNING = _map_col_a (ch 1), appended at ch 4
        EvalOperator eval = new EvalOperator(ctx, new LoadChannel(1));

        MapContractOperator contract = new MapContractOperator(
            mapPosChannel,
            mapPageIdChannel,
            4,
            new int[] { 1 },
            new int[] { 0 },
            0,
            ctx,
            tracker
        );

        Block aBlock1 = mvIntBlock(factory, 1, 2);
        Block aBlock2 = mvIntBlock(factory, 3, 4);
        Page input1 = new Page(aBlock1);
        Page input2 = new Page(aBlock2);

        List<Page> output;
        try (tracker; expand; eval; contract) {
            output = runPipeline(expand, List.of(eval), contract, List.of(input1, input2));
        }

        // Expect 2 output pages, one per input page
        assertThat(output, hasSize(2));

        // Page 1: 1 doc with RETURNING = [1, 2] (MV)
        Page out1 = output.get(0);
        assertThat(out1.getPositionCount(), equalTo(1));
        IntBlock ret1 = out1.getBlock(out1.getBlockCount() - 1);
        assertThat(ret1.getValueCount(0), equalTo(2));
        int first1 = ret1.getFirstValueIndex(0);
        assertThat(ret1.getInt(first1), equalTo(1));
        assertThat(ret1.getInt(first1 + 1), equalTo(2));

        // Page 2: 1 doc with RETURNING = [3, 4] (MV)
        Page out2 = output.get(1);
        assertThat(out2.getPositionCount(), equalTo(1));
        IntBlock ret2 = out2.getBlock(out2.getBlockCount() - 1);
        assertThat(ret2.getValueCount(0), equalTo(2));
        int first2 = ret2.getFirstValueIndex(0);
        assertThat(ret2.getInt(first2), equalTo(3));
        assertThat(ret2.getInt(first2 + 1), equalTo(4));

        for (Page p : output) {
            p.releaseBlocks();
        }
    }

    /**
     * One document with a large MV column (1000 values) so the expand output spans
     * multiple pages.  Verify all 1000 values appear in RETURNING.
     */
    public void testPartialPositionSpanningExpandedPages() {
        DriverContext ctx = driverContext();
        BlockFactory factory = ctx.blockFactory();

        MapCombinator combinator = new MapCombinator.Leaf(0, "a");
        int[] leafChannels = { 0 };
        String[] leafNames = { "a" };
        int mapPosChannel = 2;
        // Set maxPageSize small (10) so the 1000-value expansion spans 100 output pages
        int maxPageSize = 10;
        int mapPageIdChannel = 3;
        MapPageTracker tracker = new MapPageTracker(ctx.blockFactory().breaker());

        MapExpandOperator expand = new MapExpandOperator(combinator, leafChannels, leafNames, mapPosChannel, maxPageSize, ctx, tracker);

        // RETURNING = _map_col_a (ch 1), appended at ch 4
        EvalOperator eval = new EvalOperator(ctx, new LoadChannel(1));

        MapContractOperator contract = new MapContractOperator(
            mapPosChannel,
            mapPageIdChannel,
            4,
            new int[] { 1 },
            new int[] { 0 },
            0,
            ctx,
            tracker
        );

        // Build a single document with a=[0, 1, ..., 999]
        int n = 1000;
        IntBlock.Builder aBuilder = factory.newIntBlockBuilder(1);
        aBuilder.beginPositionEntry();
        for (int i = 0; i < n; i++) {
            aBuilder.appendInt(i);
        }
        aBuilder.endPositionEntry();
        Block aBlock = aBuilder.build();
        Page input = new Page(aBlock);

        List<Page> output;
        try (tracker; expand; eval; contract) {
            output = runPipeline(expand, List.of(eval), contract, List.of(input));
        }

        // Expect 1 output page with 1 position containing all 1000 values
        assertThat(output, hasSize(1));
        Page out = output.get(0);
        assertThat(out.getPositionCount(), equalTo(1));

        IntBlock ret = out.getBlock(out.getBlockCount() - 1);
        assertThat(ret.getValueCount(0), equalTo(n));
        int first = ret.getFirstValueIndex(0);
        for (int i = 0; i < n; i++) {
            assertThat(ret.getInt(first + i), equalTo(i));
        }

        for (Page p : output) {
            p.releaseBlocks();
        }
    }

    /**
     * Verifies that {@link MapCombinator#leaves()} returns leaves in left-to-right
     * depth-first order.
     */
    public void testCombinatorLeavesOrder() {
        MapCombinator.Leaf la = new MapCombinator.Leaf(0, "a");
        MapCombinator.Leaf lb = new MapCombinator.Leaf(1, "b");
        MapCombinator.Leaf lc = new MapCombinator.Leaf(2, "c");

        MapCombinator nested = new MapCombinator.Cross(la, new MapCombinator.Zip(lb, lc));
        List<MapCombinator.Leaf> leaves = nested.leaves();
        assertThat(leaves, hasSize(3));
        assertThat(leaves.get(0), equalTo(la));
        assertThat(leaves.get(1), equalTo(lb));
        assertThat(leaves.get(2), equalTo(lc));
    }
}
