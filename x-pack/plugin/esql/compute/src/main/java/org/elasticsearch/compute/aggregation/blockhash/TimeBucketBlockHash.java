/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * A {@link BlockHash} for the coordinator-side aggregate produced by time-bucketed aggregations:
 * {@code (timeBucket: LONG, label1: BYTES_REF, ..., labelN: BYTES_REF)}.
 * <p>
 * In this shape the {@code label} columns, taken together as one tuple, recur unchanged across
 * every time bucket a series appears in. {@link PackedValuesBlockHash} is wasteful here: it
 * flattens all columns into one composite byte string per row, re-copying the label bytes once per
 * distinct bucket instead of once per distinct label combination.
 * <p>
 * This class avoids that by hashing the label tuple through its own dictionary first, reducing it
 * to a small integer ordinal, and storing the raw label bytes only once per distinct combination.
 * The outer hash that assigns group IDs is then a cheap two-{@code long} hash keyed on
 * {@code (labelOrdinal, timeBucket)} — the same dictionary pattern {@link TimeSeriesBlockHash}
 * uses for {@code (tsid, timestamp)}.
 * <p>
 * Label columns must be single-valued; a multi-valued label column throws
 * {@link UnsupportedOperationException}. This is safe for the coordinator aggregate emitted by
 * {@code TranslateTimeSeriesAggregate}, where label values are collected by {@code VALUES()} over
 * per-series groups and are therefore always single-valued.
 * <p>
 * {@link #lookup} is unimplemented, mirroring {@link TimeSeriesBlockHash}, since only
 * {@code GroupingAggregatorFunction#add} is used for the aggregate use case this class targets.
 */
public final class TimeBucketBlockHash extends BlockHash {

    private static final TopNEncoder ENCODER = TopNEncoder.DEFAULT_UNSORTABLE;

    private final int timeBucketChannel;
    private final List<GroupSpec> tailSpecs;
    private final int nullTrackingBytes;

    // Package-private so tests can inspect ramBytesUsed() directly, matching the precedent set by
    // PackedValuesBlockHash#bytesRefHash.
    final BytesRefHashTable tailDictionary;
    final LongLongHashTable outerHash;
    private final BreakingBytesRefBuilder packBuffer;
    private final BytesRef scratch = new BytesRef();

    public TimeBucketBlockHash(int timeBucketChannel, List<GroupSpec> tailSpecs, BlockFactory blockFactory) {
        super(blockFactory);
        this.timeBucketChannel = timeBucketChannel;
        this.tailSpecs = tailSpecs;
        this.nullTrackingBytes = (tailSpecs.size() + 7) / 8;
        boolean success = false;
        try {
            this.tailDictionary = HashImplFactory.newBytesRefHash(blockFactory);
            this.outerHash = HashImplFactory.newLongLongHash(blockFactory);
            this.packBuffer = new BreakingBytesRefBuilder(blockFactory.breaker(), "TimeBucketBlockHash", 128);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(tailDictionary, outerHash, packBuffer);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        final LongBlock timeBucketBlock = page.getBlock(timeBucketChannel);
        final LongVector timeBuckets = timeBucketBlock.asVector();
        if (timeBuckets == null) {
            throw new IllegalStateException("Expected a non-null vector for time bucket");
        }
        final int positionCount = timeBuckets.getPositionCount();
        final Block[] tailBlocks = new Block[tailSpecs.size()];
        for (int g = 0; g < tailSpecs.size(); g++) {
            tailBlocks[g] = page.getBlock(tailSpecs.get(g).channel());
        }
        try (var groupIdsBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                long ordinal = hashOrdToGroup(tailDictionary.add(packTail(tailBlocks, p)));
                long groupId = hashOrdToGroup(outerHash.add(ordinal, timeBuckets.getLong(p)));
                groupIdsBuilder.appendInt(p, Math.toIntExact(groupId));
            }
            try (var groupIds = groupIdsBuilder.build()) {
                addInput.add(0, groupIds);
            }
        }
    }

    /** Packs one row's label values into {@link #packBuffer}, returning a view of it. */
    private BytesRef packTail(Block[] tailBlocks, int position) {
        packBuffer.clear();
        byte[] nullBitmap = new byte[nullTrackingBytes];
        for (int g = 0; g < tailBlocks.length; g++) {
            if (tailBlocks[g].isNull(position)) {
                nullBitmap[g >> 3] |= (byte) (1 << (g & 7));
            }
        }
        packBuffer.append(nullBitmap, 0, nullTrackingBytes);
        for (int g = 0; g < tailBlocks.length; g++) {
            Block block = tailBlocks[g];
            if (block.isNull(position)) {
                continue;
            }
            if (block.getValueCount(position) != 1) {
                throw new UnsupportedOperationException("TimeBucketBlockHash does not support multi-valued label columns");
            }
            int valueIndex = block.getFirstValueIndex(position);
            switch (block.elementType()) {
                case BYTES_REF -> ENCODER.encodeBytesRef(((BytesRefBlock) block).getBytesRef(valueIndex, scratch), packBuffer);
                case LONG -> ENCODER.encodeLong(((LongBlock) block).getLong(valueIndex), packBuffer);
                case INT -> ENCODER.encodeInt(((IntBlock) block).getInt(valueIndex), packBuffer);
                case DOUBLE -> ENCODER.encodeDouble(((DoubleBlock) block).getDouble(valueIndex), packBuffer);
                case BOOLEAN -> ENCODER.encodeBoolean(((BooleanBlock) block).getBoolean(valueIndex), packBuffer);
                default -> throw new IllegalStateException("unsupported label element type [" + block.elementType() + "]");
            }
        }
        return packBuffer.bytesRefView();
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        // Not needed for the STATS/aggregate use case this class targets (mirrors TimeSeriesBlockHash,
        // which has the same gap for the same reason: only GroupingAggregatorFunction#add is used here).
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys(IntVector selected) {
        final int positionCount = selected.getPositionCount();
        final Block[] result = new Block[1 + tailSpecs.size()];

        try (var b = blockFactory.newLongVectorFixedBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                b.appendLong(p, outerHash.getKey2(selected.getInt(p)));
            }
            result[0] = b.build().asBlock();
        }

        final Block.Builder[] builders = new Block.Builder[tailSpecs.size()];
        for (int g = 0; g < tailSpecs.size(); g++) {
            builders[g] = newBuilderFor(tailSpecs.get(g).elementType(), positionCount);
        }
        boolean success = false;
        try {
            BytesRef dictScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                long ordinal = outerHash.getKey1(selected.getInt(p));
                BytesRef packed = tailDictionary.get(ordinal, dictScratch);
                unpackTail(packed, builders);
            }
            for (int g = 0; g < tailSpecs.size(); g++) {
                result[1 + g] = builders[g].build();
            }
            success = true;
        } finally {
            Releasables.close(builders);
            if (success == false) {
                Releasables.close(result[0]);
            }
        }
        return result;
    }

    private Block.Builder newBuilderFor(ElementType type, int positionCount) {
        return switch (type) {
            case BYTES_REF -> blockFactory.newBytesRefBlockBuilder(positionCount);
            case LONG -> blockFactory.newLongBlockBuilder(positionCount);
            case INT -> blockFactory.newIntBlockBuilder(positionCount);
            case DOUBLE -> blockFactory.newDoubleBlockBuilder(positionCount);
            case BOOLEAN -> blockFactory.newBooleanBlockBuilder(positionCount);
            default -> throw new IllegalStateException("unsupported label element type [" + type + "]");
        };
    }

    /** Reverses {@link #packTail}: decodes one label tuple's packed bytes back into typed columns. */
    private void unpackTail(BytesRef packed, Block.Builder[] builders) {
        BytesRef cursor = new BytesRef(packed.bytes, packed.offset, packed.length);
        byte[] nullBitmap = new byte[nullTrackingBytes];
        System.arraycopy(cursor.bytes, cursor.offset, nullBitmap, 0, nullTrackingBytes);
        cursor.offset += nullTrackingBytes;
        cursor.length -= nullTrackingBytes;

        BytesRef decodeScratch = new BytesRef();
        for (int g = 0; g < tailSpecs.size(); g++) {
            boolean isNull = (nullBitmap[g >> 3] & (1 << (g & 7))) != 0;
            if (isNull) {
                builders[g].appendNull();
                continue;
            }
            switch (tailSpecs.get(g).elementType()) {
                case BYTES_REF -> ((BytesRefBlock.Builder) builders[g]).appendBytesRef(ENCODER.decodeBytesRef(cursor, decodeScratch));
                case LONG -> ((LongBlock.Builder) builders[g]).appendLong(ENCODER.decodeLong(cursor));
                case INT -> ((IntBlock.Builder) builders[g]).appendInt(ENCODER.decodeInt(cursor));
                case DOUBLE -> ((DoubleBlock.Builder) builders[g]).appendDouble(ENCODER.decodeDouble(cursor));
                case BOOLEAN -> ((BooleanBlock.Builder) builders[g]).appendBoolean(ENCODER.decodeBoolean(cursor));
                default -> throw new IllegalStateException("unsupported label element type [" + tailSpecs.get(g).elementType() + "]");
            }
        }
    }

    @Override
    public IntVector nonEmpty() {
        return blockFactory.newIntRangeVector(0, Math.toIntExact(outerHash.size()));
    }

    @Override
    public int numKeys() {
        return Math.toIntExact(outerHash.size());
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(outerHash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public String toString() {
        return "TimeBucketBlockHash{timeBucket=["
            + timeBucketChannel
            + "], labels="
            + tailSpecs.size()
            + ", entries="
            + outerHash.size()
            + ", dictionarySize="
            + tailDictionary.ramBytesUsed()
            + "b, outerHashSize="
            + outerHash.ramBytesUsed()
            + "b, totalSize="
            + (tailDictionary.ramBytesUsed() + outerHash.ramBytesUsed())
            + "b}";
    }
}
