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
 * A streamFactory that writes a sequence of bytes.
 * 可以写入一组字节数组
 * A control byte is written before each run with positive values 0 to 127 meaning 2 to 129 repetitions. If the bytes is -1 to -128, 1 to 128 literal byte values follow.
 * 正数0-27,说明有2-129个重复---我觉得应该是3-130才对.
 * 负数.表示有1-128个具体的值
 *
 * 该类的优化:将多于3个以上重复的字节使用次数+字节代替进行优化,代价就是代码难读
 */
public class RunLengthByteWriter {
  static final int MIN_REPEAT_SIZE = 3;//最少重复数据
  static final int MAX_LITERAL_SIZE = 128;//一组最多有多少个数据一起写入
  static final int MAX_REPEAT_SIZE= 127 + MIN_REPEAT_SIZE;//最多允许重复的数据多少个一起写入  最多允许写入130个----原因是要用一个字节存储size,而一个字节的范围只能是[127,-128],因此对于大于127的,要写成127,即最多允许多加MIN_REPEAT_SIZE个重复字节
  private final PositionedOutputStream output;
  private final byte[] literals = new byte[MAX_LITERAL_SIZE];
  private int numLiterals = 0;
  private boolean repeat = false;//true表示已经进入重复数据阶段
  private int tailRunLength = 0;//记录重复数据出现的次数

  public RunLengthByteWriter(PositionedOutputStream output) {
    this.output = output;
  }

  private void writeValues() throws IOException {
    if (numLiterals != 0) {
      if (repeat) {//说明此时已经重复了
        output.write(numLiterals - MIN_REPEAT_SIZE);//写入具体多少个元素,是正数,并且为什么要-MIN_REPEAT_SIZE?难道为了数据更小么,不太懂,觉得没意义
        output.write(literals, 0, 1);//写入具体重复的值
     } else {//说明此时就是将数组内容都写入进去
        output.write(-numLiterals);//多少个数组元素,是负数,说明此时不是重复
        output.write(literals, 0, numLiterals);
      }
      repeat = false;
      tailRunLength = 0;
      numLiterals = 0;
    }
  }

  public void flush() throws IOException {
    writeValues();
    output.flush();
  }

  public void write(byte value) throws IOException {
    if (numLiterals == 0) {//因为是刚开始没有数据,或者刚刚写入完
      literals[numLiterals++] = value;//因此正常添加数据
      tailRunLength = 1;
    } else if (repeat) {//说明遇见连续重复的数据了
      if (value == literals[0]) {//说明本次继续重复
        numLiterals += 1;//重复的数据+1
        if (numLiterals == MAX_REPEAT_SIZE) {//达到最多重复数据的时候进行写入
          writeValues();
        }
      } else {//说明本次已经不重复了
        writeValues();//因此立刻写入数据
        literals[numLiterals++] = value;//正常添加数据
        tailRunLength = 1;
      }
    } else {//说明数组中已经存在数据了,并且此时不是重复数据时刻
      if (value == literals[numLiterals - 1]) {//说明该值和上一个值是一样的
        tailRunLength += 1;
      } else {//说明不一样,因此重复数据次数回归1
        tailRunLength = 1;
      }
      if (tailRunLength == MIN_REPEAT_SIZE) {//说明重复数据已经达到最小值了
        if (numLiterals + 1 == MIN_REPEAT_SIZE) {//说明本次添加后,就满足重复数据的最小次数了,说明数组内容都是重复的数据,因此不需要先将不重复的数据写出去的过程
          repeat = true;//因此设置重复变量为true
          numLiterals += 1;//累加值,但是不需要向数组中真的添加值了
        } else {
          numLiterals -= MIN_REPEAT_SIZE - 1;//说明此时重复数据已经有2个了,因此刨除这2个后,将所有数据先写入进去
          writeValues();//写入
          literals[0] = value;//因此此时都是重复的数据,重复的值就是value
          repeat = true;//设置为重复
          numLiterals = MIN_REPEAT_SIZE;//此时numLiterals的数量就是最小值
        }
      } else {//说明重复的内容没有到伐值
        literals[numLiterals++] = value;//数组追加该值
        if (numLiterals == MAX_LITERAL_SIZE) {//达到最大伐值则进行数据写出
          writeValues();
        }
      }
    }
  }

  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(numLiterals);
  }

    //需要的字节缓冲区
  public long estimateMemory() {
    return output.getBufferSize() + MAX_LITERAL_SIZE;
  }
}
