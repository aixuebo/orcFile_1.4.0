/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc;

import org.apache.orc.ColumnStatistics;

/**
 * Statistics for string columns.
 * 对String类型的字段进行统计
 */
public interface StringColumnStatistics extends ColumnStatistics {
  /**
   * Get the minimum string.
   * @return the minimum
   * 最小值,该值是一个字符串
   */
  String getMinimum();

  /**
   * Get the maximum string.
   * @return the maximum
   * 最大值
   */
  String getMaximum();

  /**
   * Get the total length of all strings
   * @return the sum (total length)
   * 计算所有字符长度
   */
  long getSum();
}
