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
import org.elasticsearch.compute.operator.Operator.OperatorFactory;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Expands each document position into a set of combination rows according to a
 * {@link MapCombinator} tree.
 * <p>
 *     For each position {@code p} in the input page the operator evaluates the combinator
 *     tree to produce a "mini-table": an {@code int[numLeaves][rows]} buffer where each
 *     entry is a raw value index into the corresponding leaf's flattened (expanded) block
 *     at position {@code p} (or {@code -1} for null).
 * </p>
 * <p>Output channel layout per row:</p>
 * <ol>
 *     <li>All original input channels — the block value at position {@code p}, broadcast.</li>
 *     <li>One {@code _map_col_<name>} scalar channel per leaf — the actual scalar value
 *         for this combination row (or null if the value index is {@code -1}).</li>
 *     <li>{@code _map_pos} — the integer position {@code p}.</li>
 *     <li>{@code _map_page_id} — monotonically increasing page counter for page-boundary
 *         detection in {@link MapContractOperator}.</li>
 * </ol>
 * <p>
 *     Output pages are capped at {@code maxPageSize} rows; when a position expands into
 *     more rows than fit in the current output page the rows are emitted across multiple
 *     output pages using a partial-position continuation.
 * </p>
 */
public class MapExpandOperator implements Operator {

    /**
     * Factory for creating {@link MapExpandOperator} instances.
     *
     * @param combinator    the combinator tree
     * @param leafChannels  channel indices for each leaf in tree traversal order
     * @param leafNames     column names for each leaf
     * @param mapPosChannel the output channel index for {@code _map_pos}
     * @param maxPageSize   maximum rows per output page
     * @param tracker       the shared page tracker
     */
    public record Factory(
        MapCombinator combinator,
        int[] leafChannels,
        String[] leafNames,
        int mapPosChannel,
        int maxPageSize,
        MapPageTracker tracker
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new MapExpandOperator(combinator, leafChannels, leafNames, mapPosChannel, maxPageSize, driverContext, tracker);
        }

        @Override
        public String describe() {
            return "MapExpandOperator[combinator=" + combinator + "]";
        }
    }

    private final MapCombinator combinator;
    private final int[] leafChannels;
    private final int mapPosChannel;
    private final int maxPageSize;
    private final BlockFactory blockFactory;
    private final MapPageTracker tracker;
    private final int numLeaves;
    /**
     * Monotonically increasing page counter.  Incremented for each source page and
     * written into the {@code _map_page_id} channel of every expanded row.  The
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
     * Full expansion for the position currently being drained across output pages.
     * Dimensions: {@code [numLeaves][pendingRowCount]}.  {@code null} when no position
     * is being drained.
     */
    private int[][] pendingLeafValues;
    /**
     * Number of valid rows in {@link #pendingLeafValues}.
     */
    private int pendingRowCount;
    /**
     * Index of the next row in {@link #pendingLeafValues} to emit.
     */
    private int nextPendingRow;
    /**
     * The source position ({@code p}) that produced {@link #pendingLeafValues}.
     */
    private int pendingPosition;
    /**
     * Expanded (flattened) leaf blocks for the current input page.
     * Built once per page in {@link #addInput} and released in {@link #releaseCurrentPage}.
     */
    private Block[] expandedLeafBlocks;

    private boolean finished;

    /**
     * @param combinator    the combinator tree describing how to combine input channels
     * @param leafChannels  channel indices for each leaf, in tree traversal order matching
     *                      {@link MapCombinator#leaves()}
     * @param leafNames     column names for each leaf (kept for API symmetry; actual names
     *                      are embedded in the combinator)
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
        pendingLeafValues = null;
        pendingRowCount = 0;
        nextPendingRow = 0;
        pendingPosition = -1;
        tracker.onPageStart(page);
        pageCounter++;
        // Expand leaf blocks once per page so value indices map to positions in
        // the flattened block (needed for copyFrom in getOutput).
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

        // Accumulate up to maxPageSize rows.
        int[] broadcastPositions = new int[maxPageSize];
        int[][] leafValueIndices = new int[numLeaves][maxPageSize];
        int[] mapPosValues = new int[maxPageSize];
        int rowsInBatch = 0;

        while (rowsInBatch < maxPageSize) {
            // Drain pending rows from the current position first.
            if (pendingLeafValues != null && nextPendingRow < pendingRowCount) {
                broadcastPositions[rowsInBatch] = pendingPosition;
                for (int l = 0; l < numLeaves; l++) {
                    leafValueIndices[l][rowsInBatch] = pendingLeafValues[l][nextPendingRow];
                }
                mapPosValues[rowsInBatch] = pendingPosition;
                rowsInBatch++;
                nextPendingRow++;

                if (nextPendingRow == pendingRowCount) {
                    pendingLeafValues = null;
                }
                continue;
            }

            // Move to the next position.
            if (nextPosition >= inputPage.getPositionCount()) {
                break;
            }

            int p = nextPosition++;
            // Pre-expand this position into a dedicated scratch buffer so that a
            // single position's expansion can span multiple output pages.
            // Use a generously sized scratch buffer; positions with many values
            // will expand into it fully before being drained row-by-row.
            int maxExpansion = maxExpansionForPosition(p);
            int[][] posBuffer = new int[numLeaves][maxExpansion];
            int rowCount = combinator.expand(inputPage, p, posBuffer, 0, 0);
            if (rowCount > 0) {
                pendingLeafValues = posBuffer;
                pendingRowCount = rowCount;
                pendingPosition = p;
                nextPendingRow = 0;
            }
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

            // Broadcast original channels.
            for (int b = 0; b < inputBlockCount; b++) {
                outputBlocks[b] = inputPage.getBlock(b).filter(true, bcastSlice);
            }

            // Leaf (_map_col_*) channels: scalar blocks from expanded leaf blocks.
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

            // _map_pos channel.
            int[] mapPosSlice = actualRows < maxPageSize ? Arrays.copyOf(mapPosValues, actualRows) : mapPosValues;
            outputBlocks[inputBlockCount + numLeaves] = blockFactory.newIntArrayVector(mapPosSlice, actualRows).asBlock();

            // _map_page_id channel: constant value = pageCounter for all rows in this output page.
            outputBlocks[inputBlockCount + numLeaves + 1] = blockFactory.newConstantIntBlockWith(pageCounter, actualRows);

            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(outputBlocks);
            }
        }

        // If all positions have been processed, release the input page.
        if (nextPosition >= inputPage.getPositionCount() && pendingLeafValues == null) {
            releaseCurrentPage();
        }

        return new Page(outputBlocks);
    }

    /**
     * Computes the maximum number of expanded rows that position {@code p} can produce.
     * <p>
     *     For a cross-product tree this is the product of value counts across all leaves;
     *     for zip it is the maximum.  Rather than computing the exact bound (which would
     *     require a full tree traversal), we conservatively use the product of all leaf
     *     value counts, which is always an upper bound.
     * </p>
     */
    private int maxExpansionForPosition(int p) {
        int product = 1;
        for (int leafChannel : leafChannels) {
            int vc = Math.max(1, inputPage.getBlock(leafChannel).getValueCount(p));
            product *= vc;
        }
        return product;
    }

    private void releaseCurrentPage() {
        if (expandedLeafBlocks != null) {
            // expand() always returns a ref-counted block (incRef'd or new), so always release.
            Releasables.closeExpectNoException(expandedLeafBlocks);
            expandedLeafBlocks = null;
        }
        // Signal that all expanded rows for this source page have been emitted.
        tracker.onPageComplete();
        // Release the source page. All output has been emitted (broadcast blocks are
        // independent copies via filter()); the original blocks are no longer needed.
        if (inputPage != null) {
            inputPage.releaseBlocks();
            inputPage = null;
        }
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
