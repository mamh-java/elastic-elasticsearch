/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.ArrayList;
import java.util.List;

/**
 * Describes how multi-valued input columns are combined during MAP expansion.
 * <p>
 *     A {@link MapCombinator} forms a binary tree. Leaf nodes reference individual
 *     input channels by index; inner nodes describe how to combine the rows produced
 *     by their children. The {@link MapExpandOperator} evaluates this tree for each
 *     document position to produce the "mini-table" of expanded rows.
 * </p>
 * <ul>
 *     <li>{@link Leaf} — one input channel; produces one row per value (or one null row).</li>
 *     <li>{@link Cross} — cartesian product of the left and right child row sets.</li>
 *     <li>{@link Zip} — positional pairing; the shorter side is null-padded to match the longer.</li>
 * </ul>
 */
public sealed interface MapCombinator permits MapCombinator.Leaf, MapCombinator.Cross, MapCombinator.Zip {

    /**
     * Collects all {@link Leaf} nodes in this tree in left-to-right depth-first order.
     * The order matches the {@code leafChannels} and {@code leafNames} arrays passed to
     * {@link MapExpandOperator}.
     */
    List<Leaf> leaves();

    /**
     * Expands position {@code p} of the source {@code page} into the pre-allocated
     * output buffer {@code out}, starting at row {@code rowOffset}.
     *
     * <p>{@code out} is a 2-D array {@code int[numLeaves][capacity]} where
     * {@code out[leafIndex][rowIndex]} will receive the raw value index into the
     * corresponding expanded leaf block (or {@code -1} for null). Only the
     * leaf columns owned by this subtree are written; {@code leafOffset} is the
     * index of the first leaf column owned by this node.
     *
     * @param page       the source page
     * @param p          the document position within {@code page}
     * @param out        output buffer {@code int[totalLeaves][capacity]}
     * @param leafOffset first leaf-column index (in {@code out}) owned by this node
     * @param rowOffset  first row index in {@code out} to write into
     * @return           the number of rows written
     */
    int expand(Page page, int p, int[][] out, int leafOffset, int rowOffset);

    /**
     * References one input channel by index.
     *
     * @param channel the zero-based channel index in the input {@link org.elasticsearch.compute.data.Page}
     * @param name    the column name; used to derive the output column {@code _map_col_<name>}
     */
    record Leaf(int channel, String name) implements MapCombinator {
        @Override
        public List<Leaf> leaves() {
            return List.of(this);
        }

        @Override
        public int expand(Page page, int p, int[][] out, int leafOffset, int rowOffset) {
            Block block = page.getBlock(channel);
            int valueCount = block.getValueCount(p);
            if (valueCount == 0) {
                out[leafOffset][rowOffset] = -1;
                return 1;
            }
            int firstIdx = block.getFirstValueIndex(p);
            for (int i = 0; i < valueCount; i++) {
                out[leafOffset][rowOffset + i] = firstIdx + i;
            }
            return valueCount;
        }
    }

    /**
     * Produces the cartesian product of the row sets from {@code left} and {@code right}.
     */
    record Cross(MapCombinator left, MapCombinator right) implements MapCombinator {
        @Override
        public List<Leaf> leaves() {
            List<Leaf> result = new ArrayList<>(left.leaves());
            result.addAll(right.leaves());
            return result;
        }

        @Override
        public int expand(Page page, int p, int[][] out, int leafOffset, int rowOffset) {
            int leftLeaves = left.leaves().size();
            int rightLeaves = right.leaves().size();
            int rightLeafOffset = leafOffset + leftLeaves;

            // Step 1: expand right first so left can overwrite its columns without conflict.
            int rightCount = right.expand(page, p, out, rightLeafOffset, rowOffset);

            // Step 2: expand left (left subtree only touches left-leaf columns).
            int leftCount = left.expand(page, p, out, leafOffset, rowOffset);

            // Total rows = leftCount * rightCount.
            // The first leftCount rows already have right values for the first left row.
            // We need to replicate:
            // - Left columns: each left row i must appear rightCount times.
            // - Right columns: the right pattern already written in [rowOffset..rowOffset+rightCount-1]
            // must be repeated for each left row.
            //
            // Process in reverse order so we don't clobber un-replicated source rows.

            // Replicate left columns (process in reverse to avoid clobbering).
            for (int i = leftCount - 1; i >= 0; i--) {
                for (int lc = leafOffset; lc < leafOffset + leftLeaves; lc++) {
                    int srcVal = out[lc][rowOffset + i];
                    for (int j = 0; j < rightCount; j++) {
                        out[lc][rowOffset + i * rightCount + j] = srcVal;
                    }
                }
            }

            // Replicate right columns for left rows 1..leftCount-1.
            for (int i = 1; i < leftCount; i++) {
                for (int rc = rightLeafOffset; rc < rightLeafOffset + rightLeaves; rc++) {
                    System.arraycopy(out[rc], rowOffset, out[rc], rowOffset + i * rightCount, rightCount);
                }
            }

            return leftCount * rightCount;
        }
    }

    /**
     * Produces a positional pairing of the row sets from {@code left} and {@code right}.
     * When the two sides have different lengths the shorter is padded with null rows.
     */
    record Zip(MapCombinator left, MapCombinator right) implements MapCombinator {
        @Override
        public List<Leaf> leaves() {
            List<Leaf> result = new ArrayList<>(left.leaves());
            result.addAll(right.leaves());
            return result;
        }

        @Override
        public int expand(Page page, int p, int[][] out, int leafOffset, int rowOffset) {
            int leftLeaves = left.leaves().size();
            int rightLeaves = right.leaves().size();
            int rightLeafOffset = leafOffset + leftLeaves;

            int leftCount = left.expand(page, p, out, leafOffset, rowOffset);
            int rightCount = right.expand(page, p, out, rightLeafOffset, rowOffset);

            int len = Math.max(leftCount, rightCount);

            // Null-pad the shorter side.
            for (int lc = leafOffset; lc < leafOffset + leftLeaves; lc++) {
                for (int r = leftCount; r < len; r++) {
                    out[lc][rowOffset + r] = -1;
                }
            }
            for (int rc = rightLeafOffset; rc < rightLeafOffset + rightLeaves; rc++) {
                for (int r = rightCount; r < len; r++) {
                    out[rc][rowOffset + r] = -1;
                }
            }

            return len;
        }
    }
}
