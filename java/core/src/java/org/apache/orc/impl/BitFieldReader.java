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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.RunLengthByteReader;

/**
 * 具体格式参见BitFieldWriter
 */
public class BitFieldReader {
  private final RunLengthByteReader input;
  /** The number of bits in one item. Non-test code always uses 1. */
  private final int bitSize;//每一个value多少个bit组成,这个值是固定值
  private int current;
  private int bitsLeft;//current还剩余多少个bit没有被处理
  private final int mask;//表示bitSize个1,比如bitSize=3,则mask就表示111

  public BitFieldReader(InStream input,
                        int bitSize) {
    this.input = new RunLengthByteReader(input);//RunLengthByteReader相当于进行了一层反序列化操作
    this.bitSize = bitSize;
    mask = (1 << bitSize) - 1;//表示bitSize个1.
  }

  public void setInStream(InStream inStream) {
    this.input.setInStream(inStream);
  }

    //从流中读取一个byte,赋值给current
  private void readByte() throws IOException {
    if (input.hasNext()) {
      current = 0xff & input.next();//读取一个byte
      bitsLeft = 8;
    } else {
      throw new EOFException("Read past end of bit field from " + this);
    }
  }

    //返回一个具体的值
  public int next() throws IOException {
    int result = 0;//结果,最多32位存储结果值
    int bitsLeftToRead = bitSize;//要读取多少个bit才能离开
    while (bitsLeftToRead > bitsLeft) {//说明要读取的bit,已经超过了current中存储的
      result <<= bitsLeft;//比如剩余bitsLeft为8个bit,而value此时为101  现在变成101 0000 0000
        /**
         * ((1 << bitsLeft) - 1) 表示bitsLeft个1.因此bitsLeft=8的时候,返回值就是1111 1111
         * 因此与current进行&操作,返回的就是current最后8个bit
         * 然后与result进行|操作,填补result内容
         */
      result |= current & ((1 << bitsLeft) - 1);
      bitsLeftToRead -= bitsLeft;//减少已经处理完成的bit数
      readByte();//再次读取一个字节
    }

    if (bitsLeftToRead > 0) {//说明还需要继续从缓存的current中读取数据
      result <<= bitsLeftToRead;//扩容result的bit位,比如result为101,现在bitsLeftToRead需要4个bit,因此为101 0000
      bitsLeft -= bitsLeftToRead;//比如原来bitsLeft还有7个位置,现在减去4个,还剩下3个位置
      //  current >>> bitsLeft 因为仅剩下3个位置不需要,因此表示xxxx xxxx 变成 000 xxxx x
        //((1 << bitsLeftToRead) - 1); 表示bitsLeftToRead需要的4个1,即1111,因此两个操作& 就得到了此时需要的4个bit,然后在于result进行|处理
      result |= (current >>> bitsLeft) & ((1 << bitsLeftToRead) - 1);
    }
    return result & mask;//获取具体的值
  }

  /**
   * Unlike integer readers, where runs are encoded explicitly, in this one we have to read ahead
   * to figure out whether we have a run. Given that runs in booleans are likely it's worth it.
   * However it means we'd need to keep track of how many bytes we read, and next/nextVector won't
   * work anymore once this is called. These is trivial to fix, but these are never interspersed.
   * 该方法没有人用,因此抛弃
   */
  private boolean lastRunValue;
  private int lastRunLength = -1;
  private void readNextRun(int maxRunLength) throws IOException {
    assert bitSize == 1;
    if (lastRunLength > 0) return; // last run is not exhausted yet
    if (bitsLeft == 0) {
      readByte();
    }
    // First take care of the partial bits.
    boolean hasVal = false;
    int runLength = 0;
    if (bitsLeft != 8) {
      int partialBitsMask = (1 << bitsLeft) - 1;
      int partialBits = current & partialBitsMask;
      if (partialBits == partialBitsMask || partialBits == 0) {
        lastRunValue = (partialBits == partialBitsMask);
        if (maxRunLength <= bitsLeft) {
          lastRunLength = maxRunLength;
          return;
        }
        maxRunLength -= bitsLeft;
        hasVal = true;
        runLength = bitsLeft;
        bitsLeft = 0;
      } else {
        // There's no run in partial bits. Return whatever we have.
        int prefixBitsCount = 32 - bitsLeft;
        runLength = Integer.numberOfLeadingZeros(partialBits) - prefixBitsCount;
        lastRunValue = (runLength > 0);
        lastRunLength = Math.min(maxRunLength, lastRunValue ? runLength :
          (Integer.numberOfLeadingZeros(~(partialBits | ~partialBitsMask)) - prefixBitsCount));
        return;
      }
      assert bitsLeft == 0;
      readByte();
    }
    if (!hasVal) {
      lastRunValue = ((current >> 7) == 1);
      hasVal = true;
    }
    // Read full bytes until the run ends.
    assert bitsLeft == 8;
    while (maxRunLength >= 8
        && ((lastRunValue && (current == 0xff)) || (!lastRunValue && (current == 0)))) {
      runLength += 8;
      maxRunLength -= 8;
      readByte();
    }
    if (maxRunLength > 0) {
      int extraBits = Integer.numberOfLeadingZeros(
          lastRunValue ? (~(current | ~255)) : current) - 24;
      bitsLeft -= extraBits;
      runLength += extraBits;
    }
    lastRunLength = runLength;
  }

    /**
     * 读取数据填充到previous对象中
     * @param previous 等待填充的对象
     * @param previousLen 一共填充多少个value
     * @throws IOException
     */
  public void nextVector(LongColumnVector previous,
                         long previousLen) throws IOException {
    previous.isRepeating = true;//默认先设置他为全部重复的数据
    for (int i = 0; i < previousLen; i++) {//循环所有要填充value的数据
      if (previous.noNulls || !previous.isNull[i]) {//如果该位置不是null,则读取数据进行填充
        previous.vector[i] = next();//读取一个value填充进去
      } else {
        // The default value of null for int types in vectorized
        // processing is 1, so set that if the value is null
        previous.vector[i] = 1;//说明是null,则默认该值是1
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating
          && i > 0
          && ((previous.vector[0] != previous.vector[i]) ||//说明此时读取的值和第0个值不是相同的
          (previous.isNull[0] != previous.isNull[i]))) {
        previous.isRepeating = false;//说明值是不相同的
      }
    }
  }

    //定位文件
  public void seek(PositionProvider index) throws IOException {
    input.seek(index);//定位到该文件流的position位置
    int consumed = (int) index.getNext();//读取此时已经有多少个bit存在了
    if (consumed > 8) {//不可能大于8
      throw new IllegalArgumentException("Seek past end of byte at " +
          consumed + " in " + input);
    } else if (consumed != 0) {
      readByte();//读取一批次数据
      bitsLeft = 8 - consumed;//剩余bit数量
    } else {
      bitsLeft = 0;
    }
  }

    //跳过多少个value
  public void skip(long items) throws IOException {
    long totalBits = bitSize * items;//每一个value占用bitSize个bit,因此等于跳过多少个bit
    if (bitsLeft >= totalBits) {//说明此时剩余容量是够的,直接减就可以
      bitsLeft -= totalBits;
    } else {//此时说明容量不够
      totalBits -= bitsLeft;//先减去现有的容量
      input.skip(totalBits / 8);//先跳过若干个字节
      current = input.next();//读取一个新的8bit
      bitsLeft = (int) (8 - (totalBits % 8));//在减去剩余的bit
    }
  }

  @Override
  public String toString() {
    return "bit reader current: " + current + " bits left: " + bitsLeft +
        " bit size: " + bitSize + " from " + input;
  }

  boolean hasFullByte() {
    return bitsLeft == 8 || bitsLeft == 0;
  }

    //读取一个bit,但是只是撇,不真正删除那一个bit位置
  int peekOneBit() throws IOException {
    assert bitSize == 1;
    if (bitsLeft == 0) {
      readByte();
    }
    return (current >>> (bitsLeft - 1)) & 1;//比如当前current为1101 0011 比如bitsLeft为3,表示还剩下3个位置,因此1101 0011 无符号删除后2个bit,变成 1101 00,然后获取最后一个bit,即获取了应该获取的bit被返回
  }

   //撇一个byte去看一下
  int peekFullByte() throws IOException {
    assert bitSize == 1;
    assert bitsLeft == 8 || bitsLeft == 0;
    if (bitsLeft == 0) {
      readByte();
    }
    return current;
  }

    //在当前current缓冲池的剩余bit中,跳过若干个bit位
  void skipInCurrentByte(int bits) throws IOException {
    assert bitsLeft >= bits;
    bitsLeft -= bits;
  }
}
