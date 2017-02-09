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

// Library implementing HLL algorithm to derive approximate #distinct values(NDV). Please refer:
// 'HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm.' Flajolet et. al.
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.ops.ContextInformation;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HLLMergedStatistic extends AbstractMergedStatistic {
  private String name;
  private String inputName;
  private boolean configureComplete = false;
  private boolean mergeComplete = false;
  private Map<String, ValueHolder> hllHolder;
  private int accuracy;

  public HLLMergedStatistic (String name, String inputName) {
    this.name = name;
    this.inputName = inputName;
    this.hllHolder = new HashMap<>();
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
      ObjectHolder colHLLHolder;
      if (hllHolder.get(colName) != null) {
        colHLLHolder = (ObjectHolder) hllHolder.get(colName);
      } else {
        colHLLHolder = new ObjectHolder();
        colHLLHolder.obj = new HyperLogLog(accuracy);
        hllHolder.put(colName, colHLLHolder);
      }

      NullableVarBinaryVector hllVector = (NullableVarBinaryVector) vv;
      try {
        if (hllVector.getAccessor().isSet(0) == 1) {
          ByteArrayInputStream bais = new ByteArrayInputStream(hllVector.getAccessor().getObject(0), 0,
              vv.getBufferSize());
          HyperLogLog other = HyperLogLog.Builder.build(new DataInputStream(bais));
          ((HyperLogLog) colHLLHolder.obj).addAll(other);
        }
      } catch (Exception ex) {
        //TODO: Catch IOException/CardinalityMergeException
        //TODO: logger
      }
    }
  }

  @Override
  public Object getStat(String colName) {
    if (mergeComplete != true) {
      throw new IllegalStateException(String.format("Statistic `%s` has not completed merging statistics",
          name));
    }
    ObjectHolder colHLLHolder = (ObjectHolder) hllHolder.get(colName);
    return colHLLHolder.obj;
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
      ObjectHolder colHLLHolder = (ObjectHolder) hllHolder.get(colName);
      NullableVarBinaryVector vv = (NullableVarBinaryVector) outMapCol;
      vv.allocateNewSafe();
      HyperLogLog hll = (HyperLogLog) colHLLHolder.obj;
      try {
        vv.getMutator().setSafe(0, hll.getBytes(), 0, hll.getBytes().length);
      } catch (IOException ex) {
        // TODO: logger
      }
    }
    mergeComplete = true;
  }

  @Override
  public void configure(Object configurations) {
    ContextInformation contextInformation = (ContextInformation) configurations;
    accuracy = contextInformation.getHllAccuracy();
    configureComplete = true;
  }
}
