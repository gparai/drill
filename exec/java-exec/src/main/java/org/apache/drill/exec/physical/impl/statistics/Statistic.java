/**
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

/*
 * Base Statistics class - all statistics classes should extend this class
 */
public abstract class Statistic {
  /*
   * List of statistics used in Drill.
   */
  public static final String COLNAME = "column";
  public static final String COLTYPE = "type";
  public static final String SCHEMA = "schema";
  public static final String COMPUTED = "computed";
  public static final String STATCOUNT = "statcount";
  public static final String NNSTATCOUNT = "nonnullstatcount";
  public static final String NDV = "ndv";
  public static final String HLL_MERGE = "hll_merge";
  public static final String HLL = "hll";
  public static final String AVG_WIDTH = "avg_width";
  public static final String SUM_WIDTH = "sum_width";
}
