/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Expands each document position into a set of combination rows according to a
 * {@link MapCombinator} tree.
 * <p>
 *     For each position {@code p} in the input page the operator evaluates the combinator
 *     tree to produce a "mini-table": a list of {@code int[]} rows where each entry is a
 *     raw value index into the corresponding leaf's flattened (expanded) block at position
 *     {@code p} (or {@code -1} for null).
 * </p>
 * <p>Output channel layout per row:</p>
 * <ol>
 *     <li>All original input channels — the block value at position {@code p}, broadcast.</li>
 *     <li>One {@code _map_col_<name>} scalar channel per leaf — the actual scalar value
 *         for this combination row (or null if the value index is {@code -1}).</li>
 *     <li>{@code _map_pos} — the integer position {@code p}.</li>
 * </ol>
 * <p>
 *     Output pages are capped at {@code maxPageSize} rows; when a position expands into
 *     more rows than fit in the current output page the rows are emitted across multiple
 *     output pages.
 * </p>
 */
public class MapExpandOperator implements Operator {

    private final MapCombinator combinator;
    private final int[] leafChannels;
    private final int mapPosChannel;
    private final int maxPageSize;
    private final BlockFactory blockFactory;
    private final MapPageTracker tracker;
    private final int numLeaves;
    /**
     * Monotonically increasing page counter.  Incremented for each source page and
     * written into the {@code _map_pos+1} channel of every expanded row.  The
     * {@link MapContractOperator} reads this channel to detect source-page boundaries
     * reliably, even when rows from multiple source pages arrive at the contract after
     * the expand operator has already finished all pages.
     */
    private int pageCounter;

    /**
     * The input page currently being processed, or {@code null} if no page is queued.
     */
    private Page inputPage;
    /**
     * Index of the next position in {@link #inputPage} to process.
     */
    private int nextPosition;
    /**
     * Pre-computed mini-table for the current position (may span multiple output pages).
     * Each row is a {@code int[]} of length {@code numLeaves}; each entry is a raw value
     * index into the corresponding leaf's expanded block, or {@code -1} for null.
     */
    private List<int[]> pendingRows;
    /**
     * Index of the next row in {@link #pendingRows} to emit.
     */
    private int nextPendingRow;
    /**
     * The position ({@code p}) that produced {@link #pendingRows}.
     */
    private int pendingPosition;
    /**
     * Expanded (flattened) leaf blocks for the current input page.
     * Built lazily and released when the page is released.
     */
    private Block[] expandedLeafBlocks;

    private boolean finished;

    /**
     * @param combinator    the combinator tree describing how to combine input channels
     * @param leafChannels  channel indices for each leaf, in tree traversal order matching
     *                      {@link MapCombinator#leaves()}
     * @param leafNames     column names for each leaf (kept for symmetry with other factories;
     *                      actual names are embedded in the combinator)
     * @param mapPosChannel the output channel index where {@code _map_pos} will be written
     *                      (must equal {@code inputBlockCount + numLeaves})
     * @param maxPageSize   maximum number of rows per output page
     * @param driverContext the driver context (supplies the {@link BlockFactory})
     * @param tracker       the shared page tracker
     */
    public MapExpandOperator(
        MapCombinator combinator,
        int[] leafChannels,
        String[] leafNames,
        int mapPosChannel,
        int maxPageSize,
        DriverContext driverContext,
        MapPageTracker tracker
    ) {
        this.combinator = combinator;
        this.leafChannels = leafChannels;
        this.mapPosChannel = mapPosChannel;
        this.maxPageSize = maxPageSize;
        this.blockFactory = driverContext.blockFactory();
        this.tracker = tracker;
        this.numLeaves = leafChannels.length;
        assert maxPageSize > 0;
    }

    @Override
    public boolean needsInput() {
        return inputPage == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        assert inputPage == null : "already has an input page";
        inputPage = page;
        nextPosition = 0;
        pendingRows = null;
        nextPendingRow = 0;
        pendingPosition = -1;
        tracker.onPageStart(page);
        pageCounter++;
        // Expand leaf blocks once per page so value indices map to positions in
        // the flattened block (needed for null-block detection).
        expandedLeafBlocks = new Block[numLeaves];
        boolean success = false;
        try {
            for (int l = 0; l < numLeaves; l++) {
                expandedLeafBlocks[l] = inputPage.getBlock(leafChannels[l]).expand();
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(expandedLeafBlocks);
                inputPage = null;
            }
        }
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return inputPage != null;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && inputPage == null;
    }

    @Override
    public Page getOutput() {
        if (inputPage == null) {
            return null;
        }

        int inputBlockCount = inputPage.getBlockCount();
        // total output channels = inputBlockCount + numLeaves (_map_col_*) + 1 (_map_pos) + 1 (_map_page_id)
        int outputBlockCount = inputBlockCount + numLeaves + 2;

        // Accumulate up to maxPageSize rows
        int[] broadcastPositions = new int[maxPageSize];
        int[][] leafValueIndices = new int[numLeaves][maxPageSize];
        int[] mapPosValues = new int[maxPageSize];
        int rowsInBatch = 0;

        while (rowsInBatch < maxPageSize) {
            // Drain pending rows from the current position first
            if (pendingRows != null && nextPendingRow < pendingRows.size()) {
                int[] row = pendingRows.get(nextPendingRow++);
                broadcastPositions[rowsInBatch] = pendingPosition;
                for (int l = 0; l < numLeaves; l++) {
                    leafValueIndices[l][rowsInBatch] = row[l];
                }
                mapPosValues[rowsInBatch] = pendingPosition;
                rowsInBatch++;

                if (nextPendingRow == pendingRows.size()) {
                    pendingRows = null;
                }
                continue;
            }

            // Move to the next position
            if (nextPosition >= inputPage.getPositionCount()) {
                break;
            }

            int p = nextPosition++;
            pendingRows = evalCombinator(combinator, inputPage, p);
            pendingPosition = p;
            nextPendingRow = 0;
        }

        if (rowsInBatch == 0) {
            releaseCurrentPage();
            return null;
        }

        boolean success = false;
        Block[] outputBlocks = new Block[outputBlockCount];
        try {
            int actualRows = rowsInBatch;
            int[] bcastSlice = actualRows < maxPageSize ? Arrays.copyOf(broadcastPositions, actualRows) : broadcastPositions;

            // Broadcast original channels
            for (int b = 0; b < inputBlockCount; b++) {
                outputBlocks[b] = inputPage.getBlock(b).filter(true, bcastSlice);
            }

            // Leaf (_map_col_*) channels: scalar blocks from expanded leaf blocks
            for (int l = 0; l < numLeaves; l++) {
                Block.Builder builder = expandedLeafBlocks[l].elementType().newBlockBuilder(actualRows, blockFactory);
                boolean builderSuccess = false;
                try {
                    for (int r = 0; r < actualRows; r++) {
                        int vi = leafValueIndices[l][r];
                        if (vi == -1) {
                            builder.appendNull();
                        } else {
                            // expandedLeafBlocks[l] is already flattened (scalar positions),
                            // so position vi holds the single value we want.
                            builder.copyFrom(expandedLeafBlocks[l], vi, vi + 1);
                        }
                    }
                    outputBlocks[inputBlockCount + l] = builder.build();
                    builderSuccess = true;
                } finally {
                    if (builderSuccess == false) {
                        Releasables.closeExpectNoException(builder);
                    }
                }
            }

            // _map_pos channel
            int[] mapPosSlice = actualRows < maxPageSize ? Arrays.copyOf(mapPosValues, actualRows) : mapPosValues;
            outputBlocks[inputBlockCount + numLeaves] = blockFactory.newIntArrayVector(mapPosSlice, actualRows).asBlock();

            // _map_page_id channel: constant value = pageCounter for all rows in this output page
            outputBlocks[inputBlockCount + numLeaves + 1] = blockFactory.newConstantIntBlockWith(pageCounter, actualRows);

            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(outputBlocks);
            }
        }

        // If all positions have been processed, clear the input page
        if (nextPosition >= inputPage.getPositionCount() && pendingRows == null) {
            releaseCurrentPage();
        }

        return new Page(outputBlocks);
    }

    private void releaseCurrentPage() {
        if (expandedLeafBlocks != null) {
            // expand() always returns a ref-counted block (incRef'd or new), so always release.
            Releasables.closeExpectNoException(expandedLeafBlocks);
            expandedLeafBlocks = null;
        }
        // Signal that all expanded rows for this source page have been emitted.
        // MapContractOperator uses the resulting generation counter change to detect
        // page boundaries even when _map_pos alone is ambiguous.
        tracker.onPageComplete();
        // Release the source page. All output has been emitted (broadcast blocks are
        // independent copies via filter()); the original blocks are no longer needed.
        if (inputPage != null) {
            inputPage.releaseBlocks();
            inputPage = null;
        }
    }

    /**
     * Evaluates the combinator tree at document position {@code p} and returns a list of
     * rows. Each row is an {@code int[]} of length {@code numLeaves} where each entry is
     * a raw value index into the corresponding expanded leaf block (or {@code -1} for null).
     */
    private List<int[]> evalCombinator(MapCombinator node, Page page, int p) {
        if (node instanceof MapCombinator.Leaf leaf) {
            return evalLeaf(leaf, page, p);
        } else if (node instanceof MapCombinator.Cross cross) {
            List<int[]> left = evalCombinator(cross.left(), page, p);
            List<int[]> right = evalCombinator(cross.right(), page, p);
            return cross(left, cross.left().leaves().size(), right, cross.right().leaves().size());
        } else if (node instanceof MapCombinator.Zip zip) {
            List<int[]> left = evalCombinator(zip.left(), page, p);
            List<int[]> right = evalCombinator(zip.right(), page, p);
            return zip(left, zip.left().leaves().size(), right, zip.right().leaves().size());
        } else {
            throw new IllegalStateException("unknown combinator node: " + node);
        }
    }

    /**
     * Produces one row per value at position {@code p} in the leaf's block.
     * If the position is null, produces one row with value index {@code -1}.
     * <p>
     *     The returned value indices are positions in the <em>expanded</em> (flattened)
     *     leaf block, which is stored in {@link #expandedLeafBlocks}.
     * </p>
     */
    private static List<int[]> evalLeaf(MapCombinator.Leaf leaf, Page page, int p) {
        Block block = page.getBlock(leaf.channel());
        int valueCount = block.getValueCount(p);
        if (valueCount == 0) {
            // null position — emit one null row
            List<int[]> result = new ArrayList<>(1);
            result.add(new int[] { -1 });
            return result;
        }
        int firstIdx = block.getFirstValueIndex(p);
        List<int[]> result = new ArrayList<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            result.add(new int[] { firstIdx + i });
        }
        return result;
    }

    /**
     * Cartesian product of left rows and right rows.
     * Each output row is the concatenation of a left row and a right row.
     */
    private static List<int[]> cross(List<int[]> left, int leftLeaves, List<int[]> right, int rightLeaves) {
        List<int[]> result = new ArrayList<>(left.size() * right.size());
        for (int[] lRow : left) {
            for (int[] rRow : right) {
                int[] combined = new int[leftLeaves + rightLeaves];
                System.arraycopy(lRow, 0, combined, 0, leftLeaves);
                System.arraycopy(rRow, 0, combined, leftLeaves, rightLeaves);
                result.add(combined);
            }
        }
        return result;
    }

    /**
     * Positional zip of left rows and right rows, null-padding the shorter to the longer length.
     */
    private static List<int[]> zip(List<int[]> left, int leftLeaves, List<int[]> right, int rightLeaves) {
        int len = Math.max(left.size(), right.size());
        List<int[]> result = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            int[] lRow = i < left.size() ? left.get(i) : nullRow(leftLeaves);
            int[] rRow = i < right.size() ? right.get(i) : nullRow(rightLeaves);
            int[] combined = new int[leftLeaves + rightLeaves];
            System.arraycopy(lRow, 0, combined, 0, leftLeaves);
            System.arraycopy(rRow, 0, combined, leftLeaves, rightLeaves);
            result.add(combined);
        }
        return result;
    }

    /** Creates a row of {@code len} null entries (all {@code -1}). */
    private static int[] nullRow(int len) {
        int[] row = new int[len];
        Arrays.fill(row, -1);
        return row;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(() -> {
            if (expandedLeafBlocks != null) {
                Releasables.closeExpectNoException(expandedLeafBlocks);
                expandedLeafBlocks = null;
            }
        }, () -> {
            if (inputPage != null) {
                inputPage.releaseBlocks();
                inputPage = null;
            }
        });
    }

    @Override
    public String toString() {
        return "MapExpandOperator[combinator=" + combinator + "]";
    }
}
