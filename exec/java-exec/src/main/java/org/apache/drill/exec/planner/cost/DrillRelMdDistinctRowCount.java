/*
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
 */
package org.apache.drill.exec.planner.cost;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.physical.ScanPrel;

public class DrillRelMdDistinctRowCount extends RelMdDistinctRowCount {
  private static final DrillRelMdDistinctRowCount INSTANCE =
      new DrillRelMdDistinctRowCount();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE);

  /**
   * We need to override this method since Calcite and Drill calculate
   * joined row count in different ways. It helps avoid a case when
   * at the first time was used Drill join row count but at the second time
   * Calcite row count was used. It may happen when
   * {@link RelMdDistinctRowCount#getDistinctRowCount(Join, RelMetadataQuery,
   * ImmutableBitSet, RexNode)} method is used and after that used
   * another getDistinctRowCount method for parent rel, which just uses
   * row count of input rel node (our join rel).
   * It causes cost increase of best rel node when
   * {@link RelSubset#propagateCostImprovements} is called.
   *
   * This is a part of the fix for CALCITE-2018.
   */
  @Override
  public Double getDistinctRowCount(Join rel, RelMetadataQuery mq,
      ImmutableBitSet groupKey, RexNode predicate) {
    return getDistinctRowCount((RelNode) rel, mq, groupKey, predicate);
  }

  public Double getDistinctRowCount(RelNode rel, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
    if (rel instanceof DrillScanRel) {
      return getDistinctRowCount((DrillScanRel) rel, mq, groupKey, predicate);
    } else if (rel instanceof ScanPrel) {
      return getDistinctRowCount((ScanPrel) rel, mq, groupKey, predicate);
    } else if (rel instanceof SingleRel) {
      return mq.getDistinctRowCount(((SingleRel)rel).getInput(), groupKey, predicate);
    } else if (rel instanceof RelSubset) {
      if (((RelSubset) rel).getBest() != null) {
        return mq.getDistinctRowCount(((RelSubset)rel).getBest(), groupKey, predicate);
      } else {
        if (((RelSubset)rel).getOriginal() != null) {
          return mq.getDistinctRowCount(((RelSubset)rel).getOriginal(), groupKey, predicate);
        } else {
          return super.getDistinctRowCount(rel, mq, groupKey, predicate);
        }
      }
    } else {
      return super.getDistinctRowCount(rel, mq, groupKey, predicate);
    }
  }

  /**
   * Estimates the number of rows which would be produced by a GROUP BY on the
   * set of columns indicated by groupKey.
   * column").
   */
  private Double getDistinctRowCount(DrillScanRel scan, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
    DrillTable table = scan.getDrillTable();
    return getDistinctRowCountInternal((RelNode)scan, mq, table, groupKey, scan.getRowType(),
        mq.getRowCount(scan), predicate);
  }

  private Double getDistinctRowCount(ScanPrel scan, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
    DrillTable table = scan.getTable().unwrap(DrillTable.class);
    return getDistinctRowCountInternal(scan, mq, table, groupKey, scan.getRowType(),
        mq.getRowCount(scan), predicate);
  }

  private Double getDistinctRowCountInternal(RelNode scan, RelMetadataQuery mq, DrillTable table, ImmutableBitSet groupKey,
      RelDataType type, double rowCount, RexNode predicate) {
    double selectivity;

    if (table == null || table.getStatsTable() == null) {
      /* If there is no table or metadata (stats) table associated with scan, estimate the distinct
       * row count. Consistent with the estimation of Aggregate row count in RelMdRowCount :
       * distinctRowCount = rowCount * 10%.
       */
      return scan.getRows() * 0.1;
    }

    if (groupKey.length() == 0) {
      return rowCount;
    }

    /* If predicate is present, determine its selectivity to estimate filtered rows. Thereafter,
     * compute the number of distinct rows
     */
    selectivity = mq.getSelectivity(scan, predicate);
    DrillStatsTable md = table.getStatsTable();
    final double rc = selectivity*rowCount;
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
      /* TODO: `d` may change with predicate. There are 2 cases:
       * a) predicate on group-by column - NDV will depend on the type of predicate
       * b) predicate on non-group-by column - NDV will depend on correlation with group-by column
       * */
      s *= 1 - d / rc;
    }
    return (1 - s) * rc;
  }
}
