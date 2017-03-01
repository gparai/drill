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

import org.apache.drill.exec.vector.complex.MapVector;

public abstract class AbstractMergedStatistic extends Statistic implements MergedStatistic {
  protected String name;
  protected String inputName;
  protected State state;

  public void initialize(String name, String inputName) {
    this.name = name;
    this.inputName = inputName;
  }

  @Override
  public abstract void initialize(String inputName);

  @Override
  public abstract String getName();

  @Override
  public abstract String getInput();

  @Override
  public abstract void merge(MapVector input);

  @Override
  public abstract void setOutput(MapVector output);
}
