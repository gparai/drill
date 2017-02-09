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
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvgWidthMergedStatistic extends AbstractMergedStatistic {

  private String name;
  private String inputName;
  private boolean configureComplete = false;
  private boolean mergeComplete = false;
  private Map<String, ValueHolder> sumHolder;
  MergedStatistic types, nonNullStatCounts, statCounts;

  public AvgWidthMergedStatistic (String name, String inputName) {
    this.name = name;
    this.inputName = inputName;
    this.sumHolder = new HashMap<>();
    types = nonNullStatCounts = statCounts = null;
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
  public void merge(ValueVector input) {
    // Check the input is a Map Vector
    assert (input.getField().getType().getMinorType() == TypeProtos.MinorType.MAP);
    MapVector inputMap = (MapVector) input;
    for (ValueVector vv : inputMap) {
      String colName = vv.getField().getLastName();
      NullableFloat8Holder colSumHolder;
      if (sumHolder.get(colName) != null) {
        colSumHolder = (NullableFloat8Holder) sumHolder.get(colName);
      } else {
        colSumHolder = new NullableFloat8Holder();
        sumHolder.put(colName, colSumHolder);
      }
      Object val = vv.getAccessor().getObject(0);
      if (val != null) {
        colSumHolder.value += (double) val;
        colSumHolder.isSet = 1;
      }
    }
  }

  @Override
  public Object getStat(String colName) {
      if (mergeComplete != true) {
        throw new IllegalStateException(
            String.format("Statistic `%s` has not completed merging statistics", name));
      }
      NullableFloat8Holder colSumHolder = (NullableFloat8Holder) sumHolder.get(colName);
      return (long) (colSumHolder.value/ getRowCount(colName));
    }

  @Override
  public void setOutput(ValueVector output) {
    // Check the input is a Map Vector
    assert (output.getField().getType().getMinorType() == TypeProtos.MinorType.MAP);
    // Dependencies have been configured correctly
    assert (configureComplete == true);
    MapVector outputMap = (MapVector) output;

    for (ValueVector outMapCol : outputMap) {
      String colName = outMapCol.getField().getLastName();
      NullableFloat8Holder colSumHolder = (NullableFloat8Holder) sumHolder.get(colName);
      NullableFloat8Vector vv = (NullableFloat8Vector) outMapCol;
      vv.allocateNewSafe();
      vv.getMutator().setSafe(0, (colSumHolder.value / getRowCount(colName)));
    }
    mergeComplete = true;
  }

  @Override
  public void configure(Object configurations) {
    List<MergedStatistic> statistics = (List<MergedStatistic>) configurations;
    for (MergedStatistic statistic : statistics) {
      if (statistic.getName().equals("type")) {
        types = statistic;
      } else if (statistic.getName().equals("statcount")) {
        statCounts = statistic;
      } else if (statistic.getName().equals("nonnullstatcount")) {
        nonNullStatCounts = statistic;
      }
    }
    assert (types != null && statCounts != null && nonNullStatCounts != null);
    configureComplete = true;
  }

  private long getRowCount(String colName) {
    int type = (int) types.getStat(colName);
    // If variable type - then use the nonNullCount. Otherwise, use the Count,
    // since even NULL values take up the same space.
    if (type == TypeProtos.MinorType.VAR16CHAR.getNumber()
        || type == TypeProtos.MinorType.VARCHAR.getNumber()
        || type == TypeProtos.MinorType.VARBINARY.getNumber()) {
      return (long) nonNullStatCounts.getStat(colName);
    } else {
      return (long) statCounts.getStat(colName);
    }
  }
}
