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
 * A reader that reads a sequence of bytes. A control byte is read before
 * each run with positive values 0 to 127 meaning 3 to 130 repetitions. If the
 * byte is -1 to -128, 1 to 128 literal byte values follow.
 */
public class RunLengthByteReader {
  private InStream input;
  private final byte[] literals = new byte[RunLengthByteWriter.MAX_LITERAL_SIZE];//用于存储数据
  private int numLiterals = 0;//已经存储多少个字节
  private int used = 0;//已经消费literals队列的数据消费到哪个位置了
  private boolean repeat = false;//是否开始重复了

  public RunLengthByteReader(InStream input) {
    this.input = input;
  }

  public void setInStream(InStream input) {
    this.input = input;
  }

    /**
     * 读取一组字节数组
     * @param ignoreEof true表示文件没有数据时候不报错,false表示需要报错
     */
  private void readValues(boolean ignoreEof) throws IOException {
    int control = input.read();//读取一个字节,表示数组长度
    used = 0;
    if (control == -1) {//说明没有字节了,有可能说明数组长度是-1,即有1个字节,因此不能抛异常
      if (!ignoreEof) {
        throw new EOFException("Read past end of buffer RLE byte from " + input);
      }
      used = numLiterals = 0;
      return;
    } else if (control < 0x80) {//表示128,二进制10000000,即是一个字节,因为负数都是比128大
      repeat = true;//说明重复
      numLiterals = control + RunLengthByteWriter.MIN_REPEAT_SIZE;//总重复的数量
      int val = input.read();//读取重复的字节
      if (val == -1) {
        throw new EOFException("Reading RLE byte got EOF");
      }
      literals[0] = (byte) val;//设置具体的值
    } else {
      repeat = false;//说明不是重复
      numLiterals = 0x100 - control; //0x100表示256
      int bytes = 0;
      while (bytes < numLiterals) {//连续读取若干个字节
        int result = input.read(literals, bytes, numLiterals - bytes);//读取的字节存储到字节数组中
        if (result == -1) {
          throw new EOFException("Reading RLE byte literal got EOF in " + this);
        }
        bytes += result;
      }
    }
  }

    //说明literals字节数组还有数据,或者in还有数据
  public boolean hasNext() throws IOException {
    return used != numLiterals || input.available() > 0;
  }

    //读取下一个字节
  public byte next() throws IOException {
    byte result;
    if (used == numLiterals) {
      readValues(false);//继续读取下一批字节数组
    }
    if (repeat) {
      result = literals[0];//重复的话则每次读取第一个
    } else {//读取下一个字节
      result = literals[used];
    }
    ++used;
    return result;
  }

    //读取非null位置的值,存储到data中
  public void nextVector(ColumnVector previous, long[] data, long size)
      throws IOException {
    previous.isRepeating = true;//初始化的时候表示都是相同的
    for (int i = 0; i < size; i++) {
      if (!previous.isNull[i]) {//只要该位置不是null,就读取一个字节存储到data中
        data[i] = next();
      } else {
        // The default value of null for int types in vectorized
        // processing is 1, so set that if the value is null
        data[i] = 1;//null时候则赋予1
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating
          && i > 0
          && ((data[0] != data[i]) || //说明元素与第0个不相同
              (previous.isNull[0] != previous.isNull[i]))) {//或者不都是null
        previous.isRepeating = false;//说明这里面元素不是重复的,因此初始化为false
      }
    }
  }

  /**
   * Read the next size bytes into the data array, skipping over any slots
   * where isNull is true.
   * 读取一组size长度字节,存储到data中。注意要跳过任何null为true的值,即虽然要填满size个data长度的字节,但null的值其实是没有存储的
   * @param isNull if non-null, skip any rows where isNull[r] is true
   * @param data the array to read into
   * @param size the number of elements to read
   * @throws IOException
   */
  public void nextVector(boolean[] isNull, int[] data,
                         long size) throws IOException {
    if (isNull == null) {//说明没有null,
      for(int i=0; i < size; ++i) {//读取size个元素即可
        data[i] = next();
      }
    } else {
      for(int i=0; i < size; ++i) {
        if (!isNull[i]) {//只存储非null位置的元素
          data[i] = next();
        }
      }
    }
  }

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

    //跳过若干个字节
  public void skip(long items) throws IOException {
    while (items > 0) {
      if (used == numLiterals) {
        readValues(false);//读取下一批
      }
      long consume = Math.min(items, numLiterals - used);
      used += consume;
      items -= consume;
    }
  }

  @Override
  public String toString() {
    return "byte rle " + (repeat ? "repeat" : "literal") + " used: " +
        used + "/" + numLiterals + " from " + input;
  }
}
