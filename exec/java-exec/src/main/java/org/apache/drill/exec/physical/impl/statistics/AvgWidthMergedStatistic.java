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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.MinorType;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvgWidthMergedStatistic extends AbstractMergedStatistic {
  private Map<String, Double> sumHolder;
  ColTypeMergedStatistic types;
  NNStatCountMergedStatistic nonNullStatCounts;
  StatCountMergedStatistic statCounts;

  public AvgWidthMergedStatistic () {
    this.sumHolder = new HashMap<>();
    types = null;
    nonNullStatCounts = null;
    statCounts = null;
    state = State.INIT;
  }

  @Override
  public void initialize(String inputName) {
    super.initialize(Statistic.AVG_WIDTH, inputName);
    state = State.CONFIG;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getInput() {
    return inputName;
  }

  @Override
  public void merge(MapVector input) {
    // Check the input is a Map Vector
    assert (input.getField().getType().getMinorType() == TypeProtos.MinorType.MAP);
    for (ValueVector vv : input) {
      NullableFloat8Vector fv = (NullableFloat8Vector) vv;
      NullableFloat8Vector.Accessor accessor = fv.getAccessor();
      String colName = vv.getField().getLastName();
      double sum = 0;
      if (sumHolder.get(colName) != null) {
        sum = sumHolder.get(colName);
      }
      if (!accessor.isNull(0)) {
        sum += accessor.get(0);
        sumHolder.put(colName, sum);
      }
    }
  }

  public double getStat(String colName) {
    if (state != State.COMPLETE) {
      throw new IllegalStateException(
          String.format("Statistic `%s` has not completed merging statistics", name));
    }
    return sumHolder.get(colName)/getRowCount(colName);
  }

  @Override
  public void setOutput(MapVector output) {
    // Check the input is a Map Vector
    assert (output.getField().getType().getMinorType() == TypeProtos.MinorType.MAP);
    // Dependencies have been configured correctly
    assert (state == State.MERGE);
    for (ValueVector outMapCol : output) {
      String colName = outMapCol.getField().getLastName();
      NullableFloat8Vector vv = (NullableFloat8Vector) outMapCol;
      vv.allocateNewSafe();
      // For variable-length columns, we divide by non-null rows since NULL values do not
      // take up space. For fixed-length columns NULL values take up space.
      if (sumHolder.get(colName) != null
          && getRowCount(colName) > 0) {
        vv.getMutator().setSafe(0, sumHolder.get(colName) / getRowCount(colName));
      } else {
        vv.getMutator().setNull(0);
      }
    }
    state = State.COMPLETE;
  }

  public void configure(List<MergedStatistic> statisticList) {
    assert (state == State.CONFIG);
    for (MergedStatistic statistic : statisticList) {
      if (statistic.getName().equals(Statistic.COLTYPE)) {
        types = (ColTypeMergedStatistic) statistic;
      } else if (statistic.getName().equals(Statistic.STATCOUNT)) {
        statCounts = (StatCountMergedStatistic) statistic;
      } else if (statistic.getName().equals(Statistic.NNSTATCOUNT)) {
        nonNullStatCounts = (NNStatCountMergedStatistic) statistic;
      }
    }
    assert (types != null && statCounts != null && nonNullStatCounts != null);
    // Now config complete - moving to MERGE state
    state = State.MERGE;
  }

  private long getRowCount(String colName) {
    int type = types.getStat(colName);
    // If variable length type - then use the nonNullCount. Otherwise, use the Count,
    // since even NULL values take up the same space.
    if (type == MinorType.VAR16CHAR.getNumber()
        || type == MinorType.VARCHAR.getNumber()
        || type == MinorType.VARBINARY.getNumber()) {
      return nonNullStatCounts.getStat(colName);
    } else {
      return statCounts.getStat(colName);
    }
  }
}
