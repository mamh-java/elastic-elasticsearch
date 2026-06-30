/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link MapCombinator#expand}.
 * <p>
 *     Each test builds a minimal {@link Page} with {@link IntBlock}s, calls
 *     {@code combinator.expand(page, 0, out, 0, 0)}, and asserts on the returned
 *     count and the contents of {@code out}.
 * </p>
 */
public class MapCombinatorTests extends ComputeTestCase {

    /** Builds a single-position {@link IntBlock} with the given values; empty means null. */
    private static IntBlock intBlock(BlockFactory factory, int... values) {
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

    /** Allocates a buffer large enough for {@code rows} rows and {@code leaves} leaves. */
    private static int[][] buf(int leaves, int rows) {
        int[][] out = new int[leaves][rows];
        for (int[] row : out) {
            Arrays.fill(row, Integer.MIN_VALUE); // sentinel
        }
        return out;
    }

    public void testLeafSingleValue() {
        BlockFactory factory = blockFactory();
        Page page = new Page(intBlock(factory, 42));
        MapCombinator.Leaf leaf = new MapCombinator.Leaf(0, "a");
        int[][] out = buf(1, 4);
        int count = leaf.expand(page, 0, out, 0, 0);
        assertThat(count, equalTo(1));
        assertThat(out[0][0], equalTo(0)); // first value index in the block
        page.releaseBlocks();
    }

    public void testLeafMultiValue() {
        BlockFactory factory = blockFactory();
        Page page = new Page(intBlock(factory, 10, 20, 30));
        MapCombinator.Leaf leaf = new MapCombinator.Leaf(0, "a");
        int[][] out = buf(1, 4);
        int count = leaf.expand(page, 0, out, 0, 0);
        assertThat(count, equalTo(3));
        // firstValueIndex is 0; entries are 0, 1, 2
        assertThat(out[0][0], equalTo(0));
        assertThat(out[0][1], equalTo(1));
        assertThat(out[0][2], equalTo(2));
        page.releaseBlocks();
    }

    public void testLeafNull() {
        BlockFactory factory = blockFactory();
        Page page = new Page(intBlock(factory /* no values → null */));
        MapCombinator.Leaf leaf = new MapCombinator.Leaf(0, "a");
        int[][] out = buf(1, 4);
        int count = leaf.expand(page, 0, out, 0, 0);
        assertThat(count, equalTo(1));
        assertThat(out[0][0], equalTo(-1));
        page.releaseBlocks();
    }

    /**
     * {@code Cross(Leaf(a), Leaf(b))} with {@code a=[1,2]}, {@code b=[10,20]}.
     * Expects 4 rows: (a=0,b=0), (a=0,b=1), (a=1,b=0), (a=1,b=1)
     * (value indices, not raw values).
     */
    public void testCrossBasic() {
        BlockFactory factory = blockFactory();
        Page page = new Page(intBlock(factory, 1, 2), intBlock(factory, 10, 20));
        MapCombinator comb = new MapCombinator.Cross(new MapCombinator.Leaf(0, "a"), new MapCombinator.Leaf(1, "b"));
        int[][] out = buf(2, 8);
        int count = comb.expand(page, 0, out, 0, 0);
        assertThat(count, equalTo(4));
        // Row 0: a=firstIdx+0=0, b=firstIdx+0=0
        assertThat(out[0][0], equalTo(0));
        assertThat(out[1][0], equalTo(0));
        // Row 1: a=0, b=1
        assertThat(out[0][1], equalTo(0));
        assertThat(out[1][1], equalTo(1));
        // Row 2: a=1, b=0
        assertThat(out[0][2], equalTo(1));
        assertThat(out[1][2], equalTo(0));
        // Row 3: a=1, b=1
        assertThat(out[0][3], equalTo(1));
        assertThat(out[1][3], equalTo(1));
        page.releaseBlocks();
    }

    /**
     * {@code Cross(Leaf(a), Leaf(b))} with {@code a=[1]} (null) on left, {@code b=[10,20]}.
     * Null a means 1 row (with -1), cross 2 rows = 2 rows total.
     */
    public void testCrossWithNull() {
        BlockFactory factory = blockFactory();
        Page page = new Page(intBlock(factory /* null */), intBlock(factory, 10, 20));
        MapCombinator comb = new MapCombinator.Cross(new MapCombinator.Leaf(0, "a"), new MapCombinator.Leaf(1, "b"));
        int[][] out = buf(2, 4);
        int count = comb.expand(page, 0, out, 0, 0);
        assertThat(count, equalTo(2)); // 1 * 2
        assertThat(out[0][0], equalTo(-1)); // null a
        assertThat(out[1][0], equalTo(0));  // b=first
        assertThat(out[0][1], equalTo(-1)); // null a
        assertThat(out[1][1], equalTo(1));  // b=second
        page.releaseBlocks();
    }

    /** {@code Zip(a=[1,2], b=[10,20])} → 2 rows. */
    public void testZipBasic() {
        BlockFactory factory = blockFactory();
        Page page = new Page(intBlock(factory, 1, 2), intBlock(factory, 10, 20));
        MapCombinator comb = new MapCombinator.Zip(new MapCombinator.Leaf(0, "a"), new MapCombinator.Leaf(1, "b"));
        int[][] out = buf(2, 4);
        int count = comb.expand(page, 0, out, 0, 0);
        assertThat(count, equalTo(2));
        assertThat(out[0][0], equalTo(0));
        assertThat(out[1][0], equalTo(0));
        assertThat(out[0][1], equalTo(1));
        assertThat(out[1][1], equalTo(1));
        page.releaseBlocks();
    }

    /** {@code Zip(a=[1,2,3], b=[10,20])} → 3 rows; third right col = -1. */
    public void testZipUnequal() {
        BlockFactory factory = blockFactory();
        Page page = new Page(intBlock(factory, 1, 2, 3), intBlock(factory, 10, 20));
        MapCombinator comb = new MapCombinator.Zip(new MapCombinator.Leaf(0, "a"), new MapCombinator.Leaf(1, "b"));
        int[][] out = buf(2, 6);
        int count = comb.expand(page, 0, out, 0, 0);
        assertThat(count, equalTo(3));
        assertThat(out[0][0], equalTo(0));
        assertThat(out[1][0], equalTo(0));
        assertThat(out[0][1], equalTo(1));
        assertThat(out[1][1], equalTo(1));
        assertThat(out[0][2], equalTo(2)); // third a value
        assertThat(out[1][2], equalTo(-1)); // b is null-padded
        page.releaseBlocks();
    }

    /** {@code Zip(null, b=[10,20])} → 2 rows; left cols = -1. */
    public void testZipNullLeft() {
        BlockFactory factory = blockFactory();
        Page page = new Page(intBlock(factory /* null */), intBlock(factory, 10, 20));
        MapCombinator comb = new MapCombinator.Zip(new MapCombinator.Leaf(0, "a"), new MapCombinator.Leaf(1, "b"));
        int[][] out = buf(2, 4);
        int count = comb.expand(page, 0, out, 0, 0);
        assertThat(count, equalTo(2));
        // left: 1 row (null), padded to 2 with -1
        assertThat(out[0][0], equalTo(-1));
        assertThat(out[0][1], equalTo(-1)); // null-padded
        assertThat(out[1][0], equalTo(0));
        assertThat(out[1][1], equalTo(1));
        page.releaseBlocks();
    }

    /**
     * {@code Cross(Leaf(a), Zip(Leaf(b), Leaf(c)))} with {@code a=[1,2]}, {@code b=[10,20]},
     * {@code c=[100,200]}.
     * Zip(b,c) → 2 rows: (b=0,c=0), (b=1,c=1).
     * Cross(a, zip) → 4 rows:
     *   (a=0, b=0, c=0), (a=0, b=1, c=1), (a=1, b=0, c=0), (a=1, b=1, c=1).
     */
    public void testNestedCrossZip() {
        BlockFactory factory = blockFactory();
        Page page = new Page(intBlock(factory, 1, 2), intBlock(factory, 10, 20), intBlock(factory, 100, 200));
        MapCombinator comb = new MapCombinator.Cross(
            new MapCombinator.Leaf(0, "a"),
            new MapCombinator.Zip(new MapCombinator.Leaf(1, "b"), new MapCombinator.Leaf(2, "c"))
        );
        int[][] out = buf(3, 8);
        int count = comb.expand(page, 0, out, 0, 0);
        assertThat(count, equalTo(4));
        // Row 0: a=0, b=0, c=0
        assertThat(out[0][0], equalTo(0));
        assertThat(out[1][0], equalTo(0));
        assertThat(out[2][0], equalTo(0));
        // Row 1: a=0, b=1, c=1
        assertThat(out[0][1], equalTo(0));
        assertThat(out[1][1], equalTo(1));
        assertThat(out[2][1], equalTo(1));
        // Row 2: a=1, b=0, c=0
        assertThat(out[0][2], equalTo(1));
        assertThat(out[1][2], equalTo(0));
        assertThat(out[2][2], equalTo(0));
        // Row 3: a=1, b=1, c=1
        assertThat(out[0][3], equalTo(1));
        assertThat(out[1][3], equalTo(1));
        assertThat(out[2][3], equalTo(1));
        page.releaseBlocks();
    }

    /**
     * Fills the output buffer with a sentinel, expands, and confirms only the expected
     * range is written while the rest retains the sentinel.
     */
    public void testBufferNotOverwritten() {
        BlockFactory factory = blockFactory();
        Page page = new Page(intBlock(factory, 5, 6));
        MapCombinator.Leaf leaf = new MapCombinator.Leaf(0, "a");
        int sentinel = Integer.MIN_VALUE;
        int[][] out = buf(1, 8); // sentinel-filled by buf()
        int count = leaf.expand(page, 0, out, 0, 2); // rowOffset=2
        assertThat(count, equalTo(2));
        // Written: out[0][2] and out[0][3]
        assertThat(out[0][2], equalTo(0));
        assertThat(out[0][3], equalTo(1));
        // Before rowOffset: untouched
        assertThat(out[0][0], equalTo(sentinel));
        assertThat(out[0][1], equalTo(sentinel));
        // After written range: untouched
        assertThat(out[0][4], equalTo(sentinel));
        page.releaseBlocks();
    }
}
