package org.apache.orc.impl;

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

import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * The sections of stripe that we have read.
 * This might not match diskRange - 1 disk range can be multiple buffer chunks,
 * depending on DFS block boundaries.
 */
public class BufferChunk extends DiskRangeList {

  private static final Logger LOG =
      LoggerFactory.getLogger(BufferChunk.class);
  final ByteBuffer chunk;

  public BufferChunk(ByteBuffer chunk, long offset) {
    super(offset, offset + chunk.remaining());//第二个参数是end,因此是开始位置+剩余字节
    this.chunk = chunk;
  }

  public ByteBuffer getChunk() {
    return chunk;
  }

    //说明该区间是真的有数据内容的
  @Override
  public boolean hasData() {
    return chunk != null;
  }

  @Override
  public final String toString() {
    boolean makesSense = chunk.remaining() == (end - offset);
    return "data range [" + offset + ", " + end + "), size: " + chunk.remaining()
        + (makesSense ? "" : "(!)") + " type: " +
        (chunk.isDirect() ? "direct" : "array-backed");
  }

    //剪切原始字节数组.只要offset到end之间的内容,并且开始位置是offset+shiftBy进行读取
  @Override
  public DiskRange sliceAndShift(long offset, long end, long shiftBy) {
    assert offset <= end && offset >= this.offset && end <= this.end;
    assert offset + shiftBy >= 0;
    ByteBuffer sliceBuf = chunk.slice();//复制,只是复制offset和limit之间的数据
    int newPos = (int) (offset - this.offset);//移动到新的位置
    int newLimit = newPos + (int) (end - offset);//limit为新的位置+长度
    try {
      sliceBuf.position(newPos);
      sliceBuf.limit(newLimit);
    } catch (Throwable t) {
      LOG.error("Failed to slice buffer chunk with range" + " [" + this.offset + ", " + this.end
          + "), position: " + chunk.position() + " limit: " + chunk.limit() + ", "
          + (chunk.isDirect() ? "direct" : "array") + "; to [" + offset + ", " + end + ") "
          + t.getClass());
      throw new RuntimeException(t);
    }
    return new BufferChunk(sliceBuf, offset + shiftBy);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    }
    BufferChunk ob = (BufferChunk) other;
    return chunk.equals(ob.chunk);
  }

  @Override
  public int hashCode() {
    return chunk.hashCode();
  }

  @Override
  public ByteBuffer getData() {
    return chunk;
  }
}
