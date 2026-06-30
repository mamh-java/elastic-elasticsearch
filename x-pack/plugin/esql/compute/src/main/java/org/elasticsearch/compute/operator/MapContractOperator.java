/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Re-collapses the expanded rows produced by {@link MapExpandOperator} back into one
 * output row per original document position.
 * <p>
 *     For each source position {@code p} (encoded in the {@code _map_pos} channel), all
 *     expanded rows sharing that position are collected and their RETURNING channel values
 *     are merged into a single (possibly multi-value) block:
 * </p>
 * <ul>
 *     <li>0 accumulated non-null values → null.</li>
 *     <li>1 non-null value → scalar.</li>
 *     <li>N ≥ 2 non-null values → multi-value block.</li>
 * </ul>
 * <p>Output channel layout:</p>
 * <ol>
 *     <li>Source channels (original non-hidden columns), passed through as-is.</li>
 *     <li>RETURNING — the collapsed (possibly multi-value) result.</li>
 * </ol>
 * <p>
 *     Hidden channels ({@code _map_pos} and {@code _map_col_*}) are stripped.
 * </p>
 * <p>
 *     Page boundaries are detected via the {@link MapPageTracker}: when {@code _map_pos}
 *     decreases the operator knows a new source page has started.  On that event it
 *     finalises the previous output page and calls {@link MapPageTracker#onPageComplete()}.
 * </p>
 */
public class MapContractOperator implements Operator {

    private final int mapPosChannel;
    private final int mapPageIdChannel;
    private final int returningChannel;
    private final int[] sourceChannels;
    private final BlockFactory blockFactory;
    private final MapPageTracker tracker;

    /**
     * Last seen {@code _map_pos} value, or {@code -1} if no row has been seen for the
     * current source page.
     */
    private int lastPos;

    /**
     * The {@code _map_page_id} value of the current source page, or {@code -1} if no
     * rows have been seen yet.  Each source page gets a unique monotonically increasing
     * ID embedded in every expanded row by {@link MapExpandOperator}.  When the ID
     * changes the contract operator knows a new source page has started, even when
     * {@code _map_pos} would otherwise be ambiguous (e.g. single-document pages).
     */
    private int lastPageId = -1;

    /**
     * The position count of the source page currently being contracted.  Stored when
     * the contract operator first sees a row from a new source page.
     */
    private int currentSourcePageSize;

    /**
     * Element type of the RETURNING channel in the expanded stream.  Captured from the
     * first expanded page seen.  Used to initialise the RETURNING block builder.
     */
    private ElementType returningElementType;

    /**
     * Element types of the source channels.  Captured from the first expanded page seen.
     * Used to initialise the source-channel block builders, even when
     * {@link #pendingSourceValues} is not yet set (e.g. initial gap positions).
     */
    private ElementType[] sourceElementTypes;

    /**
     * Accumulated RETURNING blocks for the current position.
     * Each entry is a single-row block (scalar or null) taken from the RETURNING channel.
     * The blocks are owned here and must be released.
     */
    private final List<Block> pendingReturningValues;

    /**
     * Snapshot of the source-channel values for the current position.
     * Captured from the first expanded row of each position so we can safely emit them
     * after the input page has been released.
     * Length == {@code sourceChannels.length}, each entry is a 1-position block.
     */
    private Block[] pendingSourceValues;

    /**
     * Builders for the output page currently being assembled.
     * Indices 0..(sourceChannels.length-1) correspond to source channels;
     * index {@code sourceChannels.length} is the RETURNING column.
     * {@code null} when no output page is in progress.
     */
    private Block.Builder[] outputBuilders;

    private boolean finished;

    /**
     * Completed output pages waiting to be retrieved via {@link #getOutput()}.
     */
    private final List<Page> outputPages;

    /**
     * @param mapPosChannel             channel index of {@code _map_pos}
     * @param mapPageIdChannel          channel index of {@code _map_page_id} (embedded by expand)
     * @param returningChannel          channel index of the RETURNING column in the expanded stream
     * @param mapColChannels            channel indices of {@code _map_col_*} columns (unused at
     *                                  runtime; stripping is implicit via {@code sourceChannels})
     * @param sourceChannels            channel indices of original (non-hidden) columns to pass through
     * @param sourcePositionCountUnused unused parameter kept for API symmetry
     * @param driverContext             the driver context (supplies the {@link BlockFactory})
     * @param tracker                   the shared page tracker
     */
    public MapContractOperator(
        int mapPosChannel,
        int mapPageIdChannel,
        int returningChannel,
        int[] mapColChannels,
        int[] sourceChannels,
        int sourcePositionCountUnused,
        DriverContext driverContext,
        MapPageTracker tracker
    ) {
        this.mapPosChannel = mapPosChannel;
        this.mapPageIdChannel = mapPageIdChannel;
        this.returningChannel = returningChannel;
        this.sourceChannels = sourceChannels;
        this.blockFactory = driverContext.blockFactory();
        this.tracker = tracker;
        this.lastPos = -1;
        this.pendingReturningValues = new ArrayList<>();
        this.outputPages = new ArrayList<>();
    }

    @Override
    public boolean needsInput() {
        return outputPages.isEmpty() && finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            processPage(page);
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return outputPages.isEmpty() == false;
    }

    @Override
    public void finish() {
        finished = true;
        // Finalise whatever position is in flight, then fill any trailing gaps
        if (lastPos >= 0) {
            ensureOutputBuilders();
            finalizeCurrentPosition();
            fillGaps(lastPos + 1, currentSourcePageSize);
            emitOutputPage();
            lastPos = -1;
        }
    }

    @Override
    public boolean isFinished() {
        return finished && outputPages.isEmpty();
    }

    @Override
    public Page getOutput() {
        if (outputPages.isEmpty()) {
            return null;
        }
        return outputPages.remove(0);
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(() -> pendingReturningValues.forEach(Block::close), () -> {
            if (pendingSourceValues != null) {
                Releasables.closeExpectNoException(pendingSourceValues);
                pendingSourceValues = null;
            }
        }, () -> {
            if (outputBuilders != null) {
                Releasables.closeExpectNoException(outputBuilders);
                outputBuilders = null;
            }
        }, () -> outputPages.forEach(Page::releaseBlocks));
    }

    @Override
    public String toString() {
        return "MapContractOperator[mapPosChannel=" + mapPosChannel + ", returningChannel=" + returningChannel + "]";
    }

    private void processPage(Page page) {
        IntBlock mapPosBlock = page.getBlock(mapPosChannel);
        IntBlock mapPageIdBlock = page.getBlock(mapPageIdChannel);
        Block returningBlock = page.getBlock(returningChannel);

        for (int r = 0; r < page.getPositionCount(); r++) {
            int pos = mapPosBlock.getInt(mapPosBlock.getFirstValueIndex(r));
            int pageId = mapPageIdBlock.getInt(mapPageIdBlock.getFirstValueIndex(r));

            // Detect page boundary: either pos decreased, or the source page ID changed.
            // Using the page ID embedded in each expanded row ensures reliable detection
            // even when the expand operator has already processed all source pages before
            // the contract operator sees any rows.
            boolean newSourcePage = lastPageId >= 0 && pageId != lastPageId;
            if (newSourcePage) {
                // New source page started
                ensureOutputBuilders();
                finalizeCurrentPosition();
                // Fill trailing gaps using the stored size of the PREVIOUS source page.
                fillGaps(lastPos + 1, currentSourcePageSize);
                emitOutputPage();
                // Reset for the new source page
                lastPos = -1;
                lastPageId = -1;
                releasePendingSourceValues();
            }

            // If this is the very first row we've ever seen (or first of a new page),
            // capture page size, source element types, and the RETURNING element type.
            if (lastPageId < 0) {
                currentSourcePageSize = tracker.lastStartedPageSize(pageId);
                lastPageId = pageId;
                returningElementType = returningBlock.elementType();
                sourceElementTypes = new ElementType[sourceChannels.length];
                for (int i = 0; i < sourceChannels.length; i++) {
                    sourceElementTypes[i] = page.getBlock(sourceChannels[i]).elementType();
                }
            }

            if (pos > lastPos) {
                // Advanced to a new position within this source page.
                // Fill gaps for any positions between the previous and current one.
                int gapStart = (lastPos < 0) ? 0 : lastPos + 1;
                if (gapStart < pos || lastPos >= 0) {
                    ensureOutputBuilders();
                    if (lastPos >= 0) {
                        finalizeCurrentPosition();
                    }
                    fillGaps(gapStart, pos);
                }
                // Snapshot source-channel values from this row (first row for pos)
                captureSourceValues(page, r);
                lastPos = pos;
            }

            // Accumulate the RETURNING value for this row (single-row block)
            Block singleRetBlock = returningBlock.filter(false, new int[] { r });
            pendingReturningValues.add(singleRetBlock);
        }
    }

    /**
     * Captures source-channel values from the given row into {@link #pendingSourceValues}.
     * Each captured value is a 1-position block owned by this operator.
     */
    private void captureSourceValues(Page page, int row) {
        releasePendingSourceValues();
        pendingSourceValues = new Block[sourceChannels.length];
        boolean success = false;
        try {
            for (int i = 0; i < sourceChannels.length; i++) {
                pendingSourceValues[i] = page.getBlock(sourceChannels[i]).filter(false, new int[] { row });
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(pendingSourceValues);
                pendingSourceValues = null;
            }
        }
    }

    private void releasePendingSourceValues() {
        if (pendingSourceValues != null) {
            Releasables.closeExpectNoException(pendingSourceValues);
            pendingSourceValues = null;
        }
    }

    /**
     * Ensures that {@link #outputBuilders} is initialised.
     * <p>
     *     Source element types are taken from {@link #sourceElementTypes}, which is
     *     captured from the first expanded row seen.  This allows the builders to be
     *     created even before the first position's source values are captured (needed
     *     for initial-gap positions when the first seen {@code _map_pos > 0}).
     * </p>
     */
    private void ensureOutputBuilders() {
        if (outputBuilders != null || sourceElementTypes == null) {
            return;
        }
        int numBuilders = sourceChannels.length + 1; // +1 for RETURNING
        outputBuilders = new Block.Builder[numBuilders];
        int pageSize = currentSourcePageSize;
        boolean success = false;
        try {
            for (int i = 0; i < sourceChannels.length; i++) {
                outputBuilders[i] = sourceElementTypes[i].newBlockBuilder(pageSize, blockFactory);
            }
            // RETURNING builder element type — captured from the first expanded row.
            // Falls back to NULL if somehow not yet captured (e.g. empty first page).
            ElementType retType = returningElementType != null ? returningElementType : ElementType.NULL;
            outputBuilders[sourceChannels.length] = retType.newBlockBuilder(pageSize, blockFactory);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(outputBuilders);
                outputBuilders = null;
            }
        }
    }

    /**
     * Finalises the accumulated RETURNING values for {@link #lastPos} and appends one
     * row to {@link #outputBuilders}.
     */
    private void finalizeCurrentPosition() {
        if (lastPos < 0 || pendingSourceValues == null) {
            return;
        }

        // Append captured source-channel values (1-position blocks)
        for (int i = 0; i < sourceChannels.length; i++) {
            outputBuilders[i].copyFrom(pendingSourceValues[i], 0, 1);
        }

        // Merge accumulated RETURNING values into one position
        Block.Builder retBuilder = outputBuilders[sourceChannels.length];
        mergeReturningValues(retBuilder);

        // Release accumulated state
        pendingReturningValues.forEach(Block::close);
        pendingReturningValues.clear();
        releasePendingSourceValues();
    }

    /**
     * Merges all accumulated RETURNING blocks (one per surviving expanded row) into a
     * single position in {@code retBuilder}.
     * <ul>
     *     <li>0 non-null accumulated values → append null.</li>
     *     <li>1 non-null accumulated value  → copy scalar.</li>
     *     <li>N ≥ 2 non-null values          → begin MV position, copy each, end position.</li>
     * </ul>
     */
    private void mergeReturningValues(Block.Builder retBuilder) {
        // Count non-null accumulated values
        int nonNullCount = 0;
        for (Block b : pendingReturningValues) {
            if (b.getPositionCount() > 0 && b.isNull(0) == false) {
                nonNullCount++;
            }
        }

        if (nonNullCount == 0) {
            retBuilder.appendNull();
            return;
        }

        if (nonNullCount == 1) {
            // Find the single non-null block and copy it as a scalar position
            for (Block b : pendingReturningValues) {
                if (b.getPositionCount() > 0 && b.isNull(0) == false) {
                    retBuilder.copyFrom(b, 0, 1);
                    return;
                }
            }
        }

        // N >= 2 non-null values: build a multi-value position.
        // Inside beginPositionEntry() the generic copyFrom(block, 0, 1) appends the
        // scalar value to the current multi-value position without creating a new position.
        retBuilder.beginPositionEntry();
        for (Block b : pendingReturningValues) {
            if (b.getPositionCount() > 0 && b.isNull(0) == false) {
                retBuilder.copyFrom(b, 0, 1);
            }
        }
        retBuilder.endPositionEntry();
    }

    /**
     * Appends null rows for positions in the range [{@code fromPos}, {@code toPos}).
     */
    private void fillGaps(int fromPos, int toPos) {
        for (int p = fromPos; p < toPos; p++) {
            for (Block.Builder builder : outputBuilders) {
                builder.appendNull();
            }
        }
    }

    /**
     * Builds the output page from {@link #outputBuilders} and adds it to
     * {@link #outputPages}. Releases and clears {@code outputBuilders}.
     */
    private void emitOutputPage() {
        if (outputBuilders == null) {
            return;
        }
        Block[] blocks = new Block[outputBuilders.length];
        boolean success = false;
        try {
            for (int i = 0; i < outputBuilders.length; i++) {
                blocks[i] = outputBuilders[i].build();
            }
            outputPages.add(new Page(blocks));
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
        Releasables.closeExpectNoException(outputBuilders);
        outputBuilders = null;
    }
}
