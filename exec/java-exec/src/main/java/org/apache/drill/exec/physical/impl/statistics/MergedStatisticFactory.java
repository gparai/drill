/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.statistics;

public class MergedStatisticFactory {
  /*
   * Creates the appropriate statistics object given the name of the statistics and the input statistic
   */
  public static MergedStatistic getMergedStatistic(String outputStatName, String inputStatName) {
    if (outputStatName == null || inputStatName == null) {
      return null;
    } else if (outputStatName.equals(Statistic.COLNAME)) {
      return new ColumnMergedStatistic(outputStatName, inputStatName);
    } else if (outputStatName.equals(Statistic.COLTYPE)) {
      return new ColTypeMergedStatistic(outputStatName, inputStatName);
    } else if (outputStatName.equals(Statistic.STATCOUNT)) {
      return new StatCountMergedStatistic(outputStatName, inputStatName);
    } else if (outputStatName.equals(Statistic.NNSTATCOUNT)) {
      return new NNStatCountMergedStatistic(outputStatName, inputStatName);
    } else if (outputStatName.equals(Statistic.AVG_WIDTH)) {
      return new AvgWidthMergedStatistic(outputStatName, inputStatName);
    } else if (outputStatName.equals(Statistic.HLL_MERGE)) {
      return new HLLMergedStatistic(outputStatName, inputStatName);
    } else if (outputStatName.equals(Statistic.NDV)) {
      return new NDVMergedStatistic(outputStatName, inputStatName);
    } else {
      return null;
    }
  }
}

