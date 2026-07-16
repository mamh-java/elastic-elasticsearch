/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class TimeBucketBlockHashTests extends BlockHashTestCase {

    private int[] addAndCaptureGroupIds(BlockHash blockHash, Block... values) {
        int[][] captured = new int[1][];
        blockHash.add(new Page(values), new GroupingAggregatorFunction.AddInput() {
            private void addBlock(int positionOffset, IntBlock groupIds) {
                int[] ids = new int[groupIds.getPositionCount()];
                for (int p = 0; p < ids.length; p++) {
                    ids[p] = groupIds.getInt(p);
                }
                captured[0] = ids;
            }

            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addBlock(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addBlock(positionOffset, groupIds);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                addBlock(positionOffset, groupIds.asBlock());
            }

            @Override
            public void close() {
                fail("hashes should not close AddInput");
            }
        });
        return captured[0];
    }

    public void testGroupsSameTimeBucketAndLabelsTogether() {
        try (
            LongBlock timeBucket = blockFactory.newLongArrayVector(new long[] { 1, 1, 2, 2, 1 }, 5).asBlock();
            BytesRefBlock label = bytesRefBlock("a", "b", "a", "b", "a");
            BlockHash hash = new TimeBucketBlockHash(0, List.of(new BlockHash.GroupSpec(1, ElementType.BYTES_REF)), blockFactory)
        ) {
            int[] ids = addAndCaptureGroupIds(hash, timeBucket, label);
            // (timeBucket=1, label="a") occurs at positions 0 and 4 - must get the same group id.
            assertThat(ids[0], equalTo(ids[4]));
            // Every other combination must be distinct from each other and from group 0.
            assertThat(ids[1], not(equalTo(ids[0])));
            assertThat(ids[2], not(equalTo(ids[0])));
            assertThat(ids[3], not(equalTo(ids[0])));
            assertThat(ids[1], not(equalTo(ids[2])));
            assertThat(ids[1], not(equalTo(ids[3])));
            assertThat(ids[2], not(equalTo(ids[3])));
            assertThat(hash.numKeys(), equalTo(4));

            assertRoundTrip(hash, ids, new Object[][] { { 1L, "a" }, { 1L, "b" }, { 2L, "a" }, { 2L, "b" }, { 1L, "a" } });
        }
    }

    public void testMultipleLabelColumns() {
        try (
            LongBlock timeBucket = blockFactory.newLongArrayVector(new long[] { 10, 10, 20 }, 3).asBlock();
            BytesRefBlock label1 = bytesRefBlock("host-1", "host-1", "host-1");
            BytesRefBlock label2 = bytesRefBlock("cpu0", "cpu1", "cpu0");
            BlockHash hash = new TimeBucketBlockHash(
                0,
                List.of(new BlockHash.GroupSpec(1, ElementType.BYTES_REF), new BlockHash.GroupSpec(2, ElementType.BYTES_REF)),
                blockFactory
            )
        ) {
            int[] ids = addAndCaptureGroupIds(hash, timeBucket, label1, label2);
            assertThat(ids[0], not(equalTo(ids[1]))); // same bucket, different label2
            assertThat(ids[0], not(equalTo(ids[2]))); // same labels, different bucket
            assertThat(hash.numKeys(), equalTo(3));

            assertRoundTrip(hash, ids, new Object[][] { { 10L, "host-1", "cpu0" }, { 10L, "host-1", "cpu1" }, { 20L, "host-1", "cpu0" } });
        }
    }

    public void testNullLabelValue() {
        try (BytesRefBlock.Builder labelBuilder = blockFactory.newBytesRefBlockBuilder(2)) {
            labelBuilder.appendBytesRef(new BytesRef("x"));
            labelBuilder.appendNull();
            try (
                LongBlock timeBucket = blockFactory.newLongArrayVector(new long[] { 1, 1 }, 2).asBlock();
                BytesRefBlock label = labelBuilder.build();
                BlockHash hash = new TimeBucketBlockHash(0, List.of(new BlockHash.GroupSpec(1, ElementType.BYTES_REF)), blockFactory)
            ) {
                int[] ids = addAndCaptureGroupIds(hash, timeBucket, label);
                assertThat(ids[0], not(equalTo(ids[1])));
                assertThat(hash.numKeys(), equalTo(2));
                assertRoundTrip(hash, ids, new Object[][] { { 1L, "x" }, { 1L, null } });
            }
        }
    }

    public void testMultiValuedLabelThrows() {
        LongBlock timeBucket = blockFactory.newLongArrayVector(new long[] { 1 }, 1).asBlock();
        try (BytesRefBlock.Builder labelBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            labelBuilder.beginPositionEntry();
            labelBuilder.appendBytesRef(new BytesRef("a"));
            labelBuilder.appendBytesRef(new BytesRef("b"));
            labelBuilder.endPositionEntry();
            try (
                BytesRefBlock label = labelBuilder.build();
                BlockHash hash = new TimeBucketBlockHash(0, List.of(new BlockHash.GroupSpec(1, ElementType.BYTES_REF)), blockFactory)
            ) {
                expectThrows(UnsupportedOperationException.class, () -> addAndCaptureGroupIds(hash, timeBucket, label));
            } finally {
                Releasables.closeExpectNoException(timeBucket);
            }
        }
    }

    /**
     * Reproduces the actual measured shape from the PromQL wide-clause investigation: a series
     * (10-label tuple) recurring across several time buckets, unchanged. Verifies the dictionary
     * design gives a real, measured memory win over the generic flat-composite-key approach
     * ({@link PackedValuesBlockHash}) — not just a theoretical one.
     */
    public void testMemoryUsageVsPackedValuesBlockHash() {
        final int distinctLabels = 500;
        final int bucketsPerLabel = 6;
        final int labelColumnCount = 10;
        final int labelPaddingLength = 30; // realistic label length, similar to real hostmetrics dimensions

        final int rowCount = distinctLabels * bucketsPerLabel;
        long[] bucketValues = new long[rowCount];
        String[][] labelValues = new String[labelColumnCount][rowCount];
        int row = 0;
        for (int t = 0; t < distinctLabels; t++) {
            for (int b = 0; b < bucketsPerLabel; b++) {
                bucketValues[row] = b;
                for (int d = 0; d < labelColumnCount; d++) {
                    labelValues[d][row] = String.format(Locale.ROOT, "tuple-%05d-dim-%02d-%s", t, d, "x".repeat(labelPaddingLength));
                }
                row++;
            }
        }

        List<BlockHash.GroupSpec> tailSpecs = new ArrayList<>();
        for (int d = 0; d < labelColumnCount; d++) {
            tailSpecs.add(new BlockHash.GroupSpec(1 + d, ElementType.BYTES_REF));
        }
        List<BlockHash.GroupSpec> packedSpecs = new ArrayList<>();
        packedSpecs.add(new BlockHash.GroupSpec(0, ElementType.LONG));
        packedSpecs.addAll(tailSpecs);

        Block[] blocks = new Block[1 + labelColumnCount];
        blocks[0] = blockFactory.newLongArrayVector(bucketValues, rowCount).asBlock();
        for (int d = 0; d < labelColumnCount; d++) {
            blocks[1 + d] = bytesRefBlock(labelValues[d]);
        }

        long packedBytes;
        try (PackedValuesBlockHash packed = new PackedValuesBlockHash(packedSpecs, blockFactory, 1024)) {
            addAndCaptureGroupIds(packed, blocks);
            packedBytes = packed.bytesRefHash.ramBytesUsed();
        }

        long dictionaryBytes;
        try (TimeBucketBlockHash dict = new TimeBucketBlockHash(0, tailSpecs, blockFactory)) {
            addAndCaptureGroupIds(dict, blocks);
            dictionaryBytes = dict.tailDictionary.ramBytesUsed() + dict.outerHash.ramBytesUsed();
        }

        Releasables.closeExpectNoException(blocks);

        logger.info(
            "PackedValuesBlockHash={} bytes, TimeBucketBlockHash={} bytes, ratio={}",
            packedBytes,
            dictionaryBytes,
            (double) packedBytes / dictionaryBytes
        );
        // Expect roughly a bucketsPerLabel-fold reduction (6x here); assert a conservative 3x
        // floor so this doesn't flake on hash-table load-factor/rounding differences.
        assertThat((double) packedBytes / dictionaryBytes, greaterThan(3.0));
    }

    /**
     * Feeds each row's group id back into {@link BlockHash#getKeys} and checks the recovered
     * (timeBucket, label1, ..., labelN) tuple matches the original row exactly — the dictionary
     * ordinal/outer-hash round trip is lossless.
     */
    private void assertRoundTrip(BlockHash hash, int[] groupIdsPerRow, Object[][] expectedPerRow) {
        try (IntVector selected = blockFactory.newIntRangeVector(0, hash.numKeys())) {
            Block[] keys = hash.getKeys(selected);
            try {
                for (int row = 0; row < groupIdsPerRow.length; row++) {
                    int groupId = groupIdsPerRow[row];
                    Object[] expected = expectedPerRow[row];
                    assertThat("timeBucket for row " + row, ((LongBlock) keys[0]).getLong(groupId), equalTo(expected[0]));
                    for (int d = 0; d < expected.length - 1; d++) {
                        Object expectedLabel = expected[d + 1];
                        BytesRefBlock labelBlock = (BytesRefBlock) keys[1 + d];
                        if (expectedLabel == null) {
                            assertThat("label " + d + " for row " + row, labelBlock.isNull(groupId), equalTo(true));
                        } else {
                            assertThat(
                                "label " + d + " for row " + row,
                                labelBlock.getBytesRef(groupId, new BytesRef()),
                                equalTo(new BytesRef((String) expectedLabel))
                            );
                        }
                    }
                }
            } finally {
                Releasables.closeExpectNoException(keys);
            }
        }
    }

    private BytesRefBlock bytesRefBlock(String... values) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(values.length)) {
            for (String v : values) {
                builder.appendBytesRef(new BytesRef(v));
            }
            return builder.build();
        }
    }
}
