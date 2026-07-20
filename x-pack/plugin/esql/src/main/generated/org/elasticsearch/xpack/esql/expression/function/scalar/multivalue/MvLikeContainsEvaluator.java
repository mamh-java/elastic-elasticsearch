// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.lang.Class;
import java.lang.IllegalAccessException;
import java.lang.IllegalStateException;
import java.lang.InstantiationException;
import java.lang.Override;
import java.lang.String;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.function.Function;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.ConstantMethodResultSpecializer;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * {@link ExpressionEvaluator} implementation for {@link MvLike}.
 * This class is generated. Edit {@code EvaluatorImplementer} instead.
 */
public abstract class MvLikeContainsEvaluator implements ExpressionEvaluator {
  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MvLikeContainsEvaluator.class);

  private final Source source;

  private final ExpressionEvaluator field;

  private final BytesRef scratch;

  private final DriverContext driverContext;

  private Warnings warnings;

  public MvLikeContainsEvaluator(Source source, ExpressionEvaluator field, BytesRef scratch,
      DriverContext driverContext) {
    this.source = source;
    this.field = field;
    this.scratch = scratch;
    this.driverContext = driverContext;
  }

  protected abstract BytesRef literal();

  protected String pathLabel() {
    return "jit-folded";
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
        result.appendBoolean(MvLike.processContains(p, fieldBlock, literal(), this.scratch));
      }
      return result.build();
    }
  }

  @Override
  public String toString() {
    return "MvLikeContainsEvaluator[" + "field=" + field + ", literal=" + literal() + "]" + " (" + pathLabel() + ")";
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

    private final BytesRef literal;

    private final Function<DriverContext, BytesRef> scratch;

    public Factory(Source source, ExpressionEvaluator.Factory field, BytesRef literal,
        Function<DriverContext, BytesRef> scratch) {
      this.source = source;
      this.field = field;
      this.literal = literal;
      this.scratch = scratch;
    }

    @Override
    public MvLikeContainsEvaluator get(DriverContext context) {
      Optional<Class<? extends MvLikeContainsEvaluator>> constantSpecializedClassOpt = ConstantMethodResultSpecializer.SHARED.specializeReference(MvLikeContainsEvaluator.class, "literal", BytesRef.class, this.literal);
      if (constantSpecializedClassOpt.isPresent()) {
        Class<? extends MvLikeContainsEvaluator> constantSpecializedClass = constantSpecializedClassOpt.get();
        try {
          return (MvLikeContainsEvaluator) constantSpecializedClass.getConstructors()[0].newInstance(source, field.get(context), scratch.apply(context), context);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          throw new IllegalStateException("failed to construct specialized evaluator for MvLikeContainsEvaluator", e);
        }
      }
      return new Standard(source, field.get(context), scratch.apply(context), this.literal, context);
    }

    @Override
    public String toString() {
      return "MvLikeContainsEvaluator[" + "field=" + field + ", literal=" + literal + "]";
    }
  }

  /**
   * Concrete non-constant-specialized subclass used when {@link ConstantMethodResultSpecializer} returns {@code Optional.empty()}
   * (admission filter rejected the spin). The constant lives in a regular
   * instance field — no JIT-time constant folding, but the per-row work
   * runs correctly. The Factory chooses between this and the constant-specialized subclass.
   */
  public static final class Standard extends MvLikeContainsEvaluator {
    private final BytesRef literal;

    public Standard(Source source, ExpressionEvaluator field, BytesRef scratch, BytesRef literal,
        DriverContext driverContext) {
      super(source, field, scratch, driverContext);
      this.literal = literal;
    }

    @Override
    protected final BytesRef literal() {
      return literal;
    }

    @Override
    protected final String pathLabel() {
      return "standard";
    }
  }
}
