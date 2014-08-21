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

import java.util.ArrayList;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTranslatableTable;
import org.apache.drill.exec.planner.physical.DrillScanPrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ScanPrel;

import java.util.List;

public class DrillRelMdSelectivity extends RelMdSelectivity {
  private static final DrillRelMdSelectivity INSTANCE = new DrillRelMdSelectivity();
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRelMdSelectivity.class);
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.SELECTIVITY.method, INSTANCE);

  @Override
  public Double getSelectivity(RelNode rel, RelMetadataQuery mq, RexNode predicate) {
    if (rel instanceof RelSubset) {
      return getSubsetSelectivity((RelSubset) rel, mq, predicate);
    } else if (rel instanceof DrillScanRel) {
      return getScanSelectivity(rel, mq, predicate);
    } else if (rel instanceof ScanPrel) {
      return getScanSelectivity(rel, mq, predicate);
    } else if (rel instanceof DrillJoinRel) {
      return getJoinSelectivity(((DrillJoinRel)rel), mq, predicate);
    } else if (rel instanceof SingleRel) {
      return getSelectivity(((SingleRel)rel).getInput(), mq, predicate);
    } else {
      return super.getSelectivity(rel, mq, predicate);
    }
  }

  private Double getSubsetSelectivity(RelSubset rel, RelMetadataQuery mq, RexNode predicate) {
    if (rel.getBest() != null) {
      return getSelectivity(rel.getBest(), mq, predicate);
    } else {
      List<RelNode> list = rel.getRelList();
      if (list != null && list.size() > 0) {
        return getSelectivity(list.get(0), mq, predicate);
      }
    }
    //TODO: Not required? return mq.getSelectivity(((RelSubset)rel).getOriginal(), predicate);
    return RelMdUtil.guessSelectivity(predicate);
  }

  private Double getScanSelectivity(RelNode rel, RelMetadataQuery mq, RexNode predicate) {
    double ROWCOUNT_UNKNOWN = -1.0;
    GroupScan scan = null;
    PlannerSettings settings = PrelUtil.getPlannerSettings(rel.getCluster().getPlanner());
    if (rel instanceof DrillScanRel) {
      scan = ((DrillScanRel) rel).getGroupScan();
    } else if (rel instanceof ScanPrel) {
      scan = ((ScanPrel) rel).getGroupScan();
    }
    if (scan != null) {
      if (settings.isStatisticsEnabled()
          && scan instanceof DbGroupScan) {
        double filterRows = ((DbGroupScan) scan).getRowCount(predicate, rel);
        double totalRows = ((DbGroupScan) scan).getRowCount(null, rel);
        if (filterRows != ROWCOUNT_UNKNOWN &&
            totalRows != ROWCOUNT_UNKNOWN && totalRows > 0) {
          return Math.min(1.0, filterRows / totalRows);
        }
      }
    }
    // Do not mess with statistics used for DBGroupScans.
    if (rel instanceof DrillScanRel) {
      if (((DrillScanRel)rel).getDrillTable() != null &&
              ((DrillScanRel)rel).getDrillTable().getStatsTable() != null) {
        return getScanSelectivityInternal(((DrillScanRel)rel).getDrillTable(),
            predicate, rel.getRowType());
      }
    } else if (rel instanceof DrillScanPrel) {
      DrillTable table = rel.getTable().unwrap(DrillTable.class);
      if (table == null) {
        table = rel.getTable().unwrap(DrillTranslatableTable.class).getDrillTable();
      }
      if (table != null && table.getStatsTable() != null) {
        return getScanSelectivityInternal(table, predicate, rel.getRowType());
      }
    }
    return super.getSelectivity(rel, RelMetadataQuery.instance(), predicate);
  }

  private double getScanSelectivityInternal(DrillTable table, RexNode predicate, RelDataType type) {
    double sel = 1.0;
    if ((predicate == null) || predicate.isAlwaysTrue()) {
      return sel;
    }
    for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
      double orSel = 0;
      for (RexNode orPred : RelOptUtil.disjunctions(pred)) {
        //CALCITE guess
        Double guess = RelMdUtil.guessSelectivity(pred);
        if (orPred.isA(SqlKind.EQUALS)) {
          if (orPred instanceof RexCall) {
            int colIdx = -1;
            RexInputRef op = findRexInputRef(orPred);
            if (op != null) {
              colIdx = op.hashCode();
            }
            if (colIdx != -1 && colIdx < type.getFieldNames().size()) {
              String col = type.getFieldNames().get(colIdx);
              if (table.getStatsTable() != null
                      && table.getStatsTable().getNdv(col) != null) {
                orSel += 1.00 / table.getStatsTable().getNdv(col);
              }
            } else {
              orSel += guess;
              if (logger.isDebugEnabled()) {
                logger.warn(String.format("No input reference $[%s] found for predicate [%s]",
                    Integer.toString(colIdx), orPred.toString()));
              }
            }
          }
        } else {
          //Use the CALCITE guess. TODO: Use histograms for COMPARISON operator
          orSel += guess;
        }
      }
      sel *= orSel;
    }
    return sel;
  }

  private Double getJoinSelectivity(DrillJoinRel rel, RelMetadataQuery mq, RexNode predicate) {
    double sel = 1.0;
    // determine which filters apply to the left vs right
    RexNode leftPred, rightPred;
    JoinRelType joinType = rel.getJoinType();
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    int[] adjustments = new int[rel.getRowType().getFieldCount()];

    if (predicate != null) {
      RexNode pred;
      List<RexNode> leftFilters = new ArrayList<>();
      List<RexNode> rightFilters = new ArrayList<>();
      List<RexNode> joinFilters = new ArrayList<>();
      List<RexNode> predList = RelOptUtil.conjunctions(predicate);

      RelOptUtil.classifyFilters(
              rel,
              predList,
              joinType,
              joinType == JoinRelType.INNER,
              !joinType.generatesNullsOnLeft(),
              !joinType.generatesNullsOnRight(),
              joinFilters,
              leftFilters,
              rightFilters);
      leftPred =
              RexUtil.composeConjunction(rexBuilder, leftFilters, true);
      rightPred =
              RexUtil.composeConjunction(rexBuilder, rightFilters, true);
      for (RelNode child : rel.getInputs()) {
        RexNode modifiedPred = null;

        if (child == rel.getLeft()) {
          pred = leftPred;
        } else {
          pred = rightPred;
        }
        if (pred != null) {
          // convert the predicate to reference the types of the children
          modifiedPred =
                  pred.accept(new RelOptUtil.RexInputConverter(
                          rexBuilder,
                          null,
                          child.getRowType().getFieldList(),
                          adjustments));
        }
        sel *= mq.getSelectivity(child, modifiedPred);
      }
      sel *= RelMdUtil.guessSelectivity(
              RexUtil.composeConjunction(rexBuilder, joinFilters, true));
    }
    return sel;
  }

  private RexInputRef findRexInputRef(final RexNode node) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            public Void visitCall(RexCall call) {
              for (RexNode child : call.getOperands()) {
                child.accept(this);
              }
              return super.visitCall(call);
            }

            public Void visitInputRef(RexInputRef inputRef) {
              throw new Util.FoundOne(inputRef);
            }
          };
      node.accept(visitor);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (RexInputRef) e.getNode();
    }
  }
}
