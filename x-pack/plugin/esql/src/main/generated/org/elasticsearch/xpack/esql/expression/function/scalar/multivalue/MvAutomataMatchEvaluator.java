// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Override;
import java.lang.String;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link MvAutomataMatch}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public final class MvAutomataMatchEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvAutomataMatchEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator field;

  private final ByteRunAutomaton automaton;

  private final String pattern;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvAutomataMatchEvaluator(Source source, ExpressionEvaluator field,
      ByteRunAutomaton automaton, String pattern, BytesRef scratch, DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.automaton = automaton;
    this.pattern = pattern;
    this.scratch = scratch;
    this.driverContext = driverContext;
  }

  @Override
  public Block eval(Page page) {
    try (BytesRefBlock fieldBlock = (BytesRefBlock) field.eval(page)) {
      return eval(page.getPositionCount(), fieldBlock);
    }
  }

  @Override
  public long baseRamBytesUsed() {
    long baseRamBytesUsed = BASE_RAM_BYTES_USED;
    baseRamBytesUsed += field.baseRamBytesUsed();
    return baseRamBytesUsed;
  }

  public BooleanBlock eval(int positionCount, BytesRefBlock fieldBlock) {
    try(BooleanBlock.Builder result = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
      position: for (int p = 0; p < positionCount; p++) {
        result.appendBoolean(MvAutomataMatch.process(p, fieldBlock, this.automaton, this.pattern, this.scratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvAutomataMatchEvaluator[" + "field=" + field + ", pattern=" + pattern + "]";
  }

  @Override
  public void close() {
    Releasables.closeExpectNoException(field);
  }

  private Warnings warnings() {
    if (warnings == null) {
      this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
    }
    return warnings;
  }

  static class Factory implements ExpressionEvaluator.Factory {
    private final Source source;

    private final ExpressionEvaluator.Factory field;

    private final ByteRunAutomaton automaton;

    private final String pattern;

    private final Function<DriverContext, BytesRef> scratch;

    public Factory(Source source, ExpressionEvaluator.Factory field, ByteRunAutomaton automaton,
        String pattern, Function<DriverContext, BytesRef> scratch) {
      this.source = source;
      this.field = field;
      this.automaton = automaton;
      this.pattern = pattern;
      this.scratch = scratch;
    }

    @Override
    public MvAutomataMatchEvaluator get(DriverContext context) {
      return new MvAutomataMatchEvaluator(source, field.get(context), automaton, pattern, scratch.apply(context), context);
    }

    @Override
    public String toString() {
      return "MvAutomataMatchEvaluator[" + "field=" + field + ", pattern=" + pattern + "]";
    }
  }
}
