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
package org.apache.drill.exec.planner.physical;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.drill.exec.physical.impl.statistics.Statistic;
import org.apache.drill.exec.planner.logical.DrillAnalyzeRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;

import java.util.List;
import java.util.Map;

public class AnalyzePrule extends Prule {

  public static final RelOptRule INSTANCE = new AnalyzePrule();
  // List of output functions (from StatsAggBatch)
  private static final List<String> PHASE_1_FUNCTIONS = ImmutableList.of(
      Statistic.STATCOUNT,    // total number of entries in table fragment
      Statistic.NNSTATCOUNT,  // total number of non-null entries in table fragment
      Statistic.SUM_WIDTH,    // total column width across all entries in table fragment
      Statistic.HLL           // total distinct values in table fragment
    );
  // Mapping between output functions (from StatsMergeBatch) and
  // input functions (from StatsAggBatch)
  private static final Map<String, String> PHASE_2_FUNCTIONS = ImmutableMap.of(
      Statistic.STATCOUNT,    // total number of entries in the table (merged)
      Statistic.STATCOUNT,    // total number of entries in the table
      Statistic.NNSTATCOUNT,  // total number of non-null entries in the table
      Statistic.NNSTATCOUNT,  // total number of non-null entries in the table (merged)
      Statistic.AVG_WIDTH,    // average column width across all entries in the table (merged)
      Statistic.SUM_WIDTH,    // total column width across all entries in table
      Statistic.HLL_MERGE,    // total distinct values(computed using hll) in the table (merged)
      Statistic.HLL,          // total distinct values in table
      Statistic.NDV,          // total distinct values across all entries in the table (merged)
      Statistic.HLL           // total distinct values in table fragment
    );
  // List of input functions (from StatsMergeBatch) to UnpivotMapsBatch
  private static final List<String> UNPIVOT_FUNCTIONS = ImmutableList.of(
      Statistic.STATCOUNT,    // total number of entries in the table
      Statistic.NNSTATCOUNT,  // total number of non-null entries in the table
      Statistic.AVG_WIDTH,    // average column width across all entries in the table
      Statistic.HLL_MERGE,    // total distinct values(computed using hll) in the table
      Statistic.NDV           // total distinct values across all entries in the table
  );

  public AnalyzePrule() {
    super(RelOptHelper.some(DrillAnalyzeRel.class, DrillRel.DRILL_LOGICAL,
        RelOptHelper.any(RelNode.class)), "Prel.AnalyzePrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillAnalyzeRel analyze = call.rel(0);
    final RelNode input = call.rel(1);
    final SingleRel newAnalyze;
    final RelTraitSet singleDistTrait = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL)
        .plus(DrillDistributionTrait.SINGLETON);

    // Generate parallel ANALYZE plan:
    // Writer<-Unpivot<-StatsAgg(Phase2)<-Exchange<-StatsAgg(Phase1)<-Scan
    final RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL).
        plus(DrillDistributionTrait.DEFAULT);
    final RelNode convertedInput = convert(input, traits);

    final List<String> mapFields1 = Lists.newArrayList(PHASE_1_FUNCTIONS);
    final Map<String, String> mapFields2 = Maps.newHashMap(PHASE_2_FUNCTIONS);
    final List<String> mapFields3 = Lists.newArrayList(UNPIVOT_FUNCTIONS);
    mapFields1.add(0, Statistic.COLNAME);
    mapFields1.add(1, Statistic.COLTYPE);
    mapFields2.put(Statistic.COLNAME, Statistic.COLNAME);
    mapFields2.put(Statistic.COLTYPE, Statistic.COLTYPE);
    mapFields3.add(0, Statistic.COLNAME);
    mapFields3.add(1, Statistic.COLTYPE);
    // Now generate the two phase plan physical operators bottom-up:
    // STATSAGG->EXCHANGE->STATSMERGE->UNPIVOT
    final StatsAggPrel statsAggPrel = new StatsAggPrel(analyze.getCluster(), traits,
        convertedInput, PHASE_1_FUNCTIONS);
    UnionExchangePrel exch = new UnionExchangePrel(statsAggPrel.getCluster(), singleDistTrait,
        statsAggPrel);
    final StatsMergePrel statsMergePrel = new StatsMergePrel(exch.getCluster(), singleDistTrait,
        exch, mapFields2);
    newAnalyze = new UnpivotMapsPrel(statsMergePrel.getCluster(), singleDistTrait, statsMergePrel,
        mapFields3);
    call.transformTo(newAnalyze);
  }
}
