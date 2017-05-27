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

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

/**
 * A reader that reads a sequence of integers.
 * */
public class RunLengthIntegerReader implements IntegerReader {
  private InStream input;
  private final boolean signed;
  private final long[] literals = new long[RunLengthIntegerWriter.MAX_LITERAL_SIZE];
  private int numLiterals = 0;//表示literals元素个数
  private int delta = 0;//连续的value时候每一个值之间的差
  private int used = 0;//已经消费literals多少个元素了
  private boolean repeat = false;//true表示literals元素内容是重复的
  private SerializationUtils utils;

  public RunLengthIntegerReader(InStream input, boolean signed) throws IOException {
    this.input = input;
    this.signed = signed;
    this.utils = new SerializationUtils();
  }

  //读取一组字节数组
  private void readValues(boolean ignoreEof) throws IOException {
    int control = input.read();//读取size长度
    if (control == -1) {//允许是-1
      if (!ignoreEof) {
        throw new EOFException("Read past end of RLE integer from " + input);
      }
      used = numLiterals = 0;
      return;
    } else if (control < 0x80) {//说明是正数,即重复
      numLiterals = control + RunLengthIntegerWriter.MIN_REPEAT_SIZE;//具体多少个重复的元素
      used = 0;
      repeat = true;//设置为重复
      delta = input.read();//设置重复的值之间的差
      if (delta == -1) {
        throw new EOFException("End of stream in RLE Integer from " + input);
      }
      // convert from 0 to 255 to -128 to 127 by converting to a signed byte
      delta = (byte) (0 + delta);
      if (signed) {
        literals[0] = utils.readVslong(input);//读取具体的base值
      } else {
        literals[0] = utils.readVulong(input);
      }
    } else {//读取非重复的数据
      repeat = false;
      numLiterals = 0x100 - control;//非重复数据多少个
      used = 0;
      for(int i=0; i < numLiterals; ++i) {//每次读取一个
        if (signed) {
          literals[i] = utils.readVslong(input);
        } else {
          literals[i] = utils.readVulong(input);
        }
      }
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    return used != numLiterals || input.available() > 0;
  }

    //读取一个int值出来
  @Override
  public long next() throws IOException {
    long result;
    if (used == numLiterals) {//说明要重新读取下一批了
      readValues(false);
    }
    if (repeat) {
      result = literals[0] + (used++) * delta;
    } else {
      result = literals[used++];
    }
    return result;
  }

    //将数据读取后存储到data中
  @Override
  public void nextVector(ColumnVector previous,
                         long[] data,
                         int previousLen) throws IOException {
    previous.isRepeating = true;//先设置为都重复
    for (int i = 0; i < previousLen; i++) {
      if (!previous.isNull[i]) {//只要不是null,就获取一个值存储到data中
        data[i] = next();
      } else {//是null,即赋予该值为1
        // The default value of null for int type in vectorized
        // processing is 1, so set that if the value is null
        data[i] = 1;
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating
          && i > 0
          && (data[0] != data[i] || previous.isNull[0] != previous.isNull[i])) {//说明不重复
        previous.isRepeating = false;//因此设置该属性为false
      }
    }
  }

    //将数据写入到data中
  @Override
  public void nextVector(ColumnVector vector,
                         int[] data,
                         int size) throws IOException {
    if (vector.noNulls) {//说明没有null存在,则一次获取size个元素存储到data中
      for(int r=0; r < data.length && r < size; ++r) {
        data[r] = (int) next();
      }
    } else if (!(vector.isRepeating && vector.isNull[0])) {//说明不是重复都是null的值
      for(int r=0; r < data.length && r < size; ++r) {//循环size个
        if (!vector.isNull[r]) {//说明该位置不是null
          data[r] = (int) next();//读取一个元素
        } else {
          data[r] = 1;
        }
      }
    }
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed != 0) {
      // a loop is required for cases where we break the run into two parts
      while (consumed > 0) {
        readValues(false);
        used = consumed;
        consumed -= numLiterals;
      }
    } else {
      used = 0;
      numLiterals = 0;
    }
  }

    //跳过若干个数据
  @Override
  public void skip(long numValues) throws IOException {
    while (numValues > 0) {
      if (used == numLiterals) {
        readValues(false);
      }
      long consume = Math.min(numValues, numLiterals - used);
      used += consume;
      numValues -= consume;
    }
  }
}
