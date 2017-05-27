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

/**
 * A streamFactory that writes a sequence of integers. A control byte is written before
 * each run with positive values 0 to 127 meaning 3 to 130 repetitions, each
 * repetition is offset by a delta. If the control byte is -1 to -128, 1 to 128
 * literal vint values follow.
 * 该优化是将重复的数据使用重复个数+基数+delta三者组成,因此减少了存储空间----注意此时重复的数据是说每次叠加重复的差,比如10，20，30这样就叫做重复的数据
 * 正数表示重复的数据存储
 * 负数表示非重复的数据存储
 */
public class RunLengthIntegerWriter implements IntegerWriter {
  static final int MIN_REPEAT_SIZE = 3;//最小重复数量

  //两个差的区间范围
  static final int MAX_DELTA = 127;
  static final int MIN_DELTA = -128;//因为能确保差是一个字节可以存储的

  static final int MAX_LITERAL_SIZE = 128;//一组可以最多存储多少个int元素,128主要是Byte一个能表示最多为128
  private static final int MAX_REPEAT_SIZE = 127 + MIN_REPEAT_SIZE;//用于存储重复的数据
  private final PositionedOutputStream output;
  private final boolean signed;//true表示标志位,即第一个位置是1是允许的
  private final long[] literals = new long[MAX_LITERAL_SIZE];//每一个int值存储集合
  private int numLiterals = 0;//多少个int值
  private long delta = 0;
  private boolean repeat = false;
  private int tailRunLength = 0;//重复的数据次数
  private SerializationUtils utils;

  public RunLengthIntegerWriter(PositionedOutputStream output,
                         boolean signed) {
    this.output = output;
    this.signed = signed;
    this.utils = new SerializationUtils();
  }

  private void writeValues() throws IOException {
    if (numLiterals != 0) {
      if (repeat) {
        output.write(numLiterals - MIN_REPEAT_SIZE);//写入多少个重复的数据
        output.write((byte) delta);//写入差
        if (signed) {
          utils.writeVslong(output, literals[0]);//写入具体的base值
        } else {
          utils.writeVulong(output, literals[0]);
        }
      } else {
        output.write(-numLiterals);
        for(int i=0; i < numLiterals; ++i) {
          if (signed) {
            utils.writeVslong(output, literals[i]);
          } else {
            utils.writeVulong(output, literals[i]);
          }
        }
      }
      repeat = false;
      numLiterals = 0;
      tailRunLength = 0;
    }
  }

  @Override
  public void flush() throws IOException {
    writeValues();
    output.flush();
  }

  @Override
  public void write(long value) throws IOException {
    if (numLiterals == 0) {//说明第一次添加值
      literals[numLiterals++] = value;//正常添加值
      tailRunLength = 1;//添加重复次数
    } else if (repeat) {//说明已经重复了
      if (value == literals[0] + delta * numLiterals) {//说明继续重复
        numLiterals += 1;
        if (numLiterals == MAX_REPEAT_SIZE) {//说明达到了最大的次数伐值
          writeValues();
        }
      } else {//说明此时不重复
        writeValues();//先将重复的数据写入进去
        literals[numLiterals++] = value;//添加新值
        tailRunLength = 1;
      }
    } else {
      if (tailRunLength == 1) {//说明重复的已经有一个了---即此时数据有且只有一个,现在是准备加入第2个
        delta = value - literals[numLiterals - 1];//两个int做差
        if (delta < MIN_DELTA || delta > MAX_DELTA) {//不再区间
          tailRunLength = 1;//说明还是1个
        } else {
          tailRunLength = 2;//在区间说明是2个,即累加1个,这两个差是delta
        }
      } else if (value == literals[numLiterals - 1] + delta) {//说明完全相同---说明此时数据已经有2个以上了,现在要添加第3个以上
        tailRunLength += 1;//累加重复的数据
      } else {//说明差是不相同的,要重新计算差
        delta = value - literals[numLiterals - 1];//差
        if (delta < MIN_DELTA || delta > MAX_DELTA) {//在范围内
          tailRunLength = 1;
        } else {
          tailRunLength = 2;//一旦重新计算完差,说明相同的就又变成2个了,即第一个和第二个之间差相同
        }
      }
      if (tailRunLength == MIN_REPEAT_SIZE) {//达到最小的重复数据伐值
        if (numLiterals + 1 == MIN_REPEAT_SIZE) {//刚刚是最小伐值,说明数组中所有数据都是重复的
          repeat = true;
          numLiterals += 1;
        } else {//仅仅保留重复的数据,非重复的数据先写入到磁盘
          numLiterals -= MIN_REPEAT_SIZE - 1;//回退到非重复的数据位置
          long base = literals[numLiterals];
          writeValues();//写入数据
          literals[0] = base;
          repeat = true;
          numLiterals = MIN_REPEAT_SIZE;
        }
      } else {
        literals[numLiterals++] = value;//说明是正常的int,直接添加
        if (numLiterals == MAX_LITERAL_SIZE) {//达到伐值
          writeValues();
        }
      }
    }
  }

  @Override
  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(numLiterals);
  }

  @Override
  public long estimateMemory() {
    return output.getBufferSize();
  }
}
