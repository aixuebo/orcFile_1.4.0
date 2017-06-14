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
package org.apache.orc.impl;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 一个输出流,同时追加了一个随时可以记录当前输出流position位置的方法
 */
public abstract class PositionedOutputStream extends OutputStream {

  /**
   * Record the current position to the recorder.
   * 记录当前的position,去记录到PositionRecorder中
   * @param recorder the object that receives the position 什么对象去收集position位置
   * @throws IOException
   * 参数是表示可以存储long类型的offset
   * 因此该方法是说可以将当前的位置添加到PositionRecorder中去记录
   */
  public abstract void getPosition(PositionRecorder recorder
                                   ) throws IOException;

  /**
   * Get the memory size currently allocated as buffer associated with this
   * stream.
   * @return the number of bytes used by buffers.
   * 返回一个字节缓冲区的字节大小
   */
  public abstract long getBufferSize();
}
