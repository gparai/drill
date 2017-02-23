/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.exec.planner.cost;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.planner.common.DrillJoinRelBase;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTranslatableTable;

import java.util.ArrayList;
import java.util.List;

public class DrillRelMdDistinctRowCount extends RelMdDistinctRowCount{
  private static final DrillRelMdDistinctRowCount INSTANCE =
      new DrillRelMdDistinctRowCount();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE);

  @Override
  public Double getDistinctRowCount(RelNode rel, ImmutableBitSet groupKey, RexNode predicate) {
    if (rel instanceof TableScan) {
      return getDistinctRowCount((TableScan) rel, groupKey, predicate);
    } else if (rel instanceof SingleRel && !DrillRelOptUtil.guessRows(rel)) {
        return RelMetadataQuery.getDistinctRowCount(((SingleRel) rel).getInput(), groupKey,
            predicate);
    } else if (rel instanceof DrillJoinRelBase) {
      if (DrillRelOptUtil.guessRows(rel)) {
        return super.getDistinctRowCount(rel, groupKey, predicate);
      }
      //Assume ndv is unaffected by the join
      return getDistinctRowCount(((DrillJoinRelBase) rel), groupKey, predicate);
    } else if (rel instanceof RelSubset && !DrillRelOptUtil.guessRows(rel)) {
      if (((RelSubset) rel).getBest() != null) {
        return RelMetadataQuery.getDistinctRowCount(((RelSubset) rel).getBest(), groupKey,
            predicate);
      } else if (((RelSubset) rel).getOriginal() != null) {
        return RelMetadataQuery.getDistinctRowCount(((RelSubset) rel).getOriginal(), groupKey,
            predicate);
      } else {
        return super.getDistinctRowCount(rel, groupKey, predicate);
      }
    } else {
      return super.getDistinctRowCount(rel, groupKey, predicate);
    }
  }

  /**
   * Estimates the number of rows which would be produced by a GROUP BY on the
   * set of columns indicated by groupKey.
   * column").
   */
  private Double getDistinctRowCount(TableScan scan, ImmutableBitSet groupKey,
      RexNode predicate) {
    // If guessing, return selectivity from RelMDDistinctRowCount
    if (DrillRelOptUtil.guessRows(scan)) {
      return super.getDistinctRowCount(scan, groupKey, predicate);
    }
    DrillTable table = scan.getTable().unwrap(DrillTable.class);
    if (table == null) {
      table = scan.getTable().unwrap(DrillTranslatableTable.class).getDrillTable();
    }
    return getDistinctRowCountInternal(scan, table, groupKey, scan.getRowType(), predicate);
  }

  private Double getDistinctRowCountInternal(RelNode scan, DrillTable table,
      ImmutableBitSet groupKey, RelDataType type, RexNode predicate) {
    double selectivity, rowCount;

    if (table == null
        || table.getStatsTable() == null
        || !table.getStatsTable().isMaterialized()) {
      /* If there is no table or metadata (stats) table associated with scan, estimate the
       * distinct row count. Consistent with the estimation of Aggregate row count in
       * RelMdRowCount: distinctRowCount = rowCount * 10%.
       */
      return scan.getRows() * 0.1;
    }

    /* If predicate is present, determine its selectivity to estimate filtered rows.
     * Thereafter, compute the number of distinct rows.
     */
    selectivity = RelMetadataQuery.getSelectivity(scan, predicate);
    rowCount = RelMetadataQuery.getRowCount(scan);

    if (groupKey.length() == 0) {
      return selectivity*rowCount;
    }

    DrillStatsTable md = table.getStatsTable();
    double s = 1.0;

    for (int i = 0; i < groupKey.length(); i++) {
      final String colName = type.getFieldNames().get(i);
      // Skip NDV, if not available
      if (!groupKey.get(i)) {
        continue;
      }
      Double d = md.getNdv(colName);
      if (d == null) {
        continue;
      }
      s *= 1 - d / rowCount;
    }
    if (s < 0) {  /* rowCount maybe less than NDV(different source), sanity check */
      return selectivity * rowCount;
    } else {
      return (1 - s) * selectivity * rowCount;
    }
  }

  public Double getDistinctRowCount(DrillJoinRelBase joinRel, ImmutableBitSet groupKey,
       RexNode predicate) {
    if (DrillRelOptUtil.guessRows(joinRel)) {
      return super.getDistinctRowCount(joinRel, groupKey, predicate);
    }
    // Assume NDV is unaffected by the join when groupKey comes from one side of the join
    // Alleviates NDV over-estimates
    ImmutableBitSet.Builder leftMask = ImmutableBitSet.builder();
    ImmutableBitSet.Builder rightMask = ImmutableBitSet.builder();
    JoinRelType joinType = joinRel.getJoinType();
    RelNode left = joinRel.getInputs().get(0);
    RelNode right = joinRel.getInputs().get(1);
    RelMdUtil.setLeftRightBitmaps(groupKey, leftMask, rightMask,
        left.getRowType().getFieldCount());
    RexNode leftPred = null;
    RexNode rightPred = null;

    // Identify predicates which can be pushed onto the left and right sides of the join
    if (predicate != null) {
      ArrayList leftFilters = new ArrayList();
      ArrayList rightFilters = new ArrayList();
      ArrayList joinFilters = new ArrayList();
      List predList = RelOptUtil.conjunctions(predicate);
      RelOptUtil.classifyFilters(joinRel, predList, joinType, joinType == JoinRelType.INNER,
          !joinType.generatesNullsOnLeft(), !joinType.generatesNullsOnRight(), joinFilters,
              leftFilters, rightFilters);
      RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
      leftPred = RexUtil.composeConjunction(rexBuilder, leftFilters, true);
      rightPred = RexUtil.composeConjunction(rexBuilder, rightFilters, true);
    }

    Double leftDistRowCount = null;
    Double rightDistRowCount = null;
    double distRowCount = 1;
    ImmutableBitSet lmb = leftMask.build();
    ImmutableBitSet rmb = rightMask.build();
    // Get NDV estimates for the left and right side predicates, if applicable
    if (lmb.length() > 0) {
      leftDistRowCount = RelMetadataQuery.getDistinctRowCount(left, lmb, leftPred);
      if (leftDistRowCount != null) {
        distRowCount = leftDistRowCount.doubleValue();
      }
    }
    if (rmb.length() > 0) {
      rightDistRowCount = RelMetadataQuery.getDistinctRowCount(right, rmb, rightPred);
      if (rightDistRowCount != null) {
        distRowCount = rightDistRowCount.doubleValue();
      }
    }
    // Use max of NDVs from both sides of the join, if applicable
    if (leftDistRowCount != null && rightDistRowCount != null) {
      distRowCount = Math.max(leftDistRowCount, rightDistRowCount);
    }
    return RelMdUtil.numDistinctVals(distRowCount, RelMetadataQuery.getRowCount(joinRel));
  }
}
