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

/**
 * Dynamic int array that uses primitive types and chunks to avoid copying
 * large number of integers when it resizes.
 *
 * The motivation for this class is memory optimization, i.e. space efficient
 * storage of potentially huge arrays without good a-priori size guesses.
 *
 * The API of this class is between a primitive array and a AbstractList. It's
 * not a Collection implementation because it handles primitive types, but the
 * API could be extended to support iterators and the like.
 *
 * NOTE: Like standard Collection implementations/arrays, this class is not
 * synchronized.
 * 动态的int数组,存储int
 */
public final class DynamicIntArray {
  static final int DEFAULT_CHUNKSIZE = 8 * 1024;//8k 默认每一个数据块多少个int
  static final int INIT_CHUNKS = 128;//默认chunk数据块数量

  private int[][] data;              // the real data 真正存储数据的二维数组,第一维度表示数据块数量,第二维度表示每一个数据块内字节数组
  private int length;                // max set element index +1 最大的元素
  private int initializedChunks = 0; // the number of created chunks//已经创建到第几个数据块了
  private final int chunkSize;       // our allocation size 每一个数据块真实可以存储多少个int

  public DynamicIntArray() {
    this(DEFAULT_CHUNKSIZE);
  }

  public DynamicIntArray(int chunkSize) {
    this.chunkSize = chunkSize;

    data = new int[INIT_CHUNKS][];
  }

  /**
   * Ensure that the given index is valid.
   * 增加数据块到chunkIndex个
   */
  private void grow(int chunkIndex) {
    if (chunkIndex >= initializedChunks) {
      if (chunkIndex >= data.length) {
        int newSize = Math.max(chunkIndex + 1, 2 * data.length);
        int[][] newChunk = new int[newSize][];
        System.arraycopy(data, 0, newChunk, 0, data.length);
        data = newChunk;
      }
      for (int i=initializedChunks; i <= chunkIndex; ++i) {
        data[i] = new int[chunkSize];
      }
      initializedChunks = chunkIndex + 1;
    }
  }

  //获取index对应的数据
  public int get(int index) {
    if (index >= length) {
      throw new IndexOutOfBoundsException("Index " + index +
                                            " is outside of 0.." +
                                            (length - 1));
    }
    int i = index / chunkSize;
    int j = index % chunkSize;
    return data[i][j];
  }

  public void set(int index, int value) {
    int i = index / chunkSize;
    int j = index % chunkSize;
    grow(i);
    if (index >= length) {
      length = index + 1;//length就是最后一个index+1,即下一个位置
    }
    data[i][j] = value;
  }

  public void increment(int index, int value) {
    int i = index / chunkSize;
    int j = index % chunkSize;
    grow(i);
    if (index >= length) {
      length = index + 1;
    }
    data[i][j] += value;
  }

    //向后追加一个value
  public void add(int value) {
    int i = length / chunkSize;
    int j = length % chunkSize;
    grow(i);
    data[i][j] = value;
    length += 1;
  }

  public int size() {
    return length;
  }

  public void clear() {
    length = 0;
    for(int i=0; i < data.length; ++i) {
      data[i] = null;
    }
    initializedChunks = 0;
  }

  public String toString() {
    int i;
    StringBuilder sb = new StringBuilder(length * 4);

    sb.append('{');
    int l = length - 1;
    for (i=0; i<l; i++) {//将每一个int打印出来
      sb.append(get(i));
      sb.append(',');
    }
    sb.append(get(i));
    sb.append('}');

    return sb.toString();
  }

  public int getSizeInBytes() {
    return 4 * initializedChunks * chunkSize;
  }//占用内存字节数
}

