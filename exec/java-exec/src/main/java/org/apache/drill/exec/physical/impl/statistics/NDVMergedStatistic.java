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
package org.apache.drill.exec.physical.impl.statistics;

// Library implementing HLL algorithm to derive approximate #distinct values(NDV). Please refer:
// 'HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm.' Flajolet et. al.
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;

public class NDVMergedStatistic extends AbstractMergedStatistic {
  private Map<String, HyperLogLog> hllHolder;
  private long accuracy;
  private MergedStatistic targetTypeStatistic;

  public NDVMergedStatistic () {
    this.hllHolder = new HashMap<>();
    state = State.INIT;
  }

  public static class NDVConfiguration {
    private final OptionManager optionManager;
    private final List<MergedStatistic> dependencies;

    public NDVConfiguration (OptionManager optionsManager, List<MergedStatistic> statistics) {
      this.optionManager = optionsManager;
      this.dependencies = statistics;
    }
  }

  @Override
  public void initialize(String inputName) {
    super.initialize(Statistic.NDV, inputName);
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
    // Dependencies have been configured correctly
    assert (state == State.MERGE);
    for (ValueVector vv : input) {
      String colName = vv.getField().getName();
      HyperLogLog colHLLHolder = null;
      if (hllHolder.get(colName) != null) {
        colHLLHolder = hllHolder.get(colName);
      }
      NullableVarBinaryVector hllVector = (NullableVarBinaryVector) vv;
      NullableVarBinaryVector.Accessor accessor = hllVector.getAccessor();

      try {
        if (!accessor.isNull(0)) {
          ByteArrayInputStream bais = new ByteArrayInputStream(accessor.get(0), 0, vv.getBufferSize());
          HyperLogLog other = HyperLogLog.Builder.build(new DataInputStream(bais));
          if (colHLLHolder != null) {
            colHLLHolder.addAll(other);
            hllHolder.put(colName, colHLLHolder);
          } else {
            hllHolder.put(colName, other);
          }
        }
      } catch (CardinalityMergeException ex) {
        throw new IllegalStateException("Failed to merge the NDV statistics");
      } catch (Exception ex) {
        throw new IllegalStateException(ex);
      }
    }
  }

  public long getStat(String colName) {
    if (state != State.COMPLETE) {
      throw new IllegalStateException(String.format("Statistic `%s` has not completed merging statistics", name));
    }
    return hllHolder.get(colName).cardinality();
  }

  @Override
  public void setOutput(MapVector output) {
    // Check the input is a Map Vector
    assert (output.getField().getType().getMinorType() == TypeProtos.MinorType.MAP);
    // Dependencies have been configured correctly
    assert (state == State.MERGE);
    for (ValueVector outMapCol : output) {
      String colName = outMapCol.getField().getName();
      HyperLogLog colHLLHolder = hllHolder.get(colName);
      NullableBigIntVector vv = (NullableBigIntVector) outMapCol;
      vv.allocateNewSafe();
      if (colHLLHolder != null) {
        vv.getMutator().setSafe(0, 1, colHLLHolder.cardinality());
      } else {
        vv.getMutator().setNull(0);
      }
    }
    state = State.COMPLETE;
  }

  public void configure(NDVConfiguration ndvConfig) {
    assert (state == State.CONFIG);
    accuracy = ndvConfig.optionManager.getLong(ExecConstants.HLL_ACCURACY);
    for (MergedStatistic statistic : ndvConfig.dependencies) {
      if (statistic instanceof AvgWidthMergedStatistic) {
        targetTypeStatistic = statistic;
        break;
      }
    }
    assert (targetTypeStatistic != null);
    // Now config complete - moving to MERGE state
    state = State.MERGE;
  }

  public String getMajorTypeFromStatistic() {
    // Dependencies have been configured correctly
    assert (state == State.MERGE);
    return targetTypeStatistic.getInput();
  }
}
