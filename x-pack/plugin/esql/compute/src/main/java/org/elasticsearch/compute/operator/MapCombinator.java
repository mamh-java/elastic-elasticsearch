/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

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
    }
}
