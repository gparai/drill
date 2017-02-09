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
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;

import java.util.HashMap;
import java.util.Map;

public class NNStatCountMergedStatistic extends AbstractMergedStatistic {

  private String name;
  private String inputName;
  private boolean mergeComplete = false;
  private Map<String, ValueHolder> sumHolder;

  public NNStatCountMergedStatistic (String name, String inputName) {
    this.name = name;
    this.inputName = inputName;
    this.sumHolder = new HashMap<>();
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
      BigIntHolder colSumHolder;
      if (sumHolder.get(colName) != null) {
        colSumHolder = (BigIntHolder) sumHolder.get(colName);
      } else {
        colSumHolder = new BigIntHolder();
        sumHolder.put(colName, colSumHolder);
      }
      Object val = vv.getAccessor().getObject(0);
      if (val != null) {
        colSumHolder.value += (long) val;
      }
    }
  }

  @Override
  public Object getStat(String colName) {
    if (mergeComplete != true) {
      throw new IllegalStateException(String.format("Statistic `%s` has not completed merging statistics",
          name));
    }
    BigIntHolder colSumHolder = (BigIntHolder) sumHolder.get(colName);
    return colSumHolder.value;
  }

  @Override
  public void setOutput(ValueVector output) {
    // Check the input is a Map Vector
    assert (output.getField().getType().getMinorType() == TypeProtos.MinorType.MAP);
    MapVector outputMap = (MapVector) output;
    for (ValueVector outMapCol : outputMap) {
      String colName = outMapCol.getField().getLastName();
      BigIntHolder holder = (BigIntHolder) sumHolder.get(colName);
      NullableBigIntVector vv = (NullableBigIntVector) outMapCol;
      vv.allocateNewSafe();
      vv.getMutator().setSafe(0, holder);
    }
    mergeComplete = true;
  }
}
