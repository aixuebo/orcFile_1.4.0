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

import org.apache.orc.CompressionCodec;
import org.apache.orc.PhysicalWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 对真正的输出流进一步包装
 */
public class OutStream extends PositionedOutputStream {

  public static final int HEADER_SIZE = 3;//头文件大小,用于压缩算法存在的时候,设置压缩算法信息
  private final String name;//包装的名字
  private final PhysicalWriter.OutputReceiver receiver;//对应真正的输出流

  /**
   * Stores the uncompressed bytes that have been serialized, but not
   * compressed yet. When this fills, we compress the entire buffer.
   */
  private ByteBuffer current = null;//当前的缓冲区,未压缩的内容存储在这里面

  /**
   * Stores the compressed bytes until we have a full buffer and then outputs
   * them to the receiver. If no compression is being done, this (and overflow)
   * will always be null and the current buffer will be sent directly to the
   * receiver.
   */
  private ByteBuffer compressed = null;//存储压缩的内容

  /**
   * Since the compressed buffer may start with contents from previous
   * compression blocks, we allocate an overflow buffer so that the
   * output of the codec can be split between the two buffers. After the
   * compressed buffer is sent to the receiver, the overflow buffer becomes
   * the new compressed buffer.
   */
  private ByteBuffer overflow = null;
  private final int bufferSize;
  private final CompressionCodec codec;//压缩算法
  private long compressedBytes = 0;
  private long uncompressedBytes = 0;//已经读取了多少个未压缩的字节

  public OutStream(String name,
                   int bufferSize,
                   CompressionCodec codec,
                   PhysicalWriter.OutputReceiver receiver) throws IOException {
    this.name = name;
    this.bufferSize = bufferSize;
    this.codec = codec;
    this.receiver = receiver;
  }

  public void clear() throws IOException {
    flush();
  }

  /**
   * Write the length of the compressed bytes. Life is much easier if the
   * header is constant length, so just use 3 bytes. Considering most of the
   * codecs want between 32k (snappy) and 256k (lzo, zlib), 3 bytes should
   * be plenty. We also use the low bit for whether it is the original or
   * compressed bytes.
   * @param buffer the buffer to write the header to
   * @param position the position in the buffer to write at
   * @param val the size in the file
   * @param original is it uncompressed
   * 设置头文件---本次压缩产生了多少个字节,即压缩后的字节数,使用3个字节就够了,因为大多数压缩后也不大
   */
  private static void writeHeader(ByteBuffer buffer,
                                  int position,//从哪个位置开始写入
                                  int val,//具体的值
                                  boolean original) {//true表示原始内容,未压缩,false表示压缩内容
    buffer.put(position, (byte) ((val << 1) + (original ? 1 : 0)));
    buffer.put(position + 1, (byte) (val >> 7));
    buffer.put(position + 2, (byte) (val >> 15));
  }

    //获取输入流
  private void getNewInputBuffer() throws IOException {
    if (codec == null) {
      current = ByteBuffer.allocate(bufferSize);
    } else {
      current = ByteBuffer.allocate(bufferSize + HEADER_SIZE);
      writeHeader(current, 0, bufferSize, true);//写入头文件
      current.position(HEADER_SIZE);
    }
  }

  /**
   * Allocate a new output buffer if we are compressing.
   * 如果需要压缩,要创建写的输出流
   */
  private ByteBuffer getNewOutputBuffer() throws IOException {
    return ByteBuffer.allocate(bufferSize + HEADER_SIZE);
  }

  private void flip() throws IOException {
    current.limit(current.position());
    current.position(codec == null ? 0 : HEADER_SIZE);//跳过头,直接到数据部分
  }

    //写入未压缩的字节i
  @Override
  public void write(int i) throws IOException {
    if (current == null) {//创建新的流
      getNewInputBuffer();
    }
    if (current.remaining() < 1) {
      spill();//溢出
    }
    uncompressedBytes += 1;
    current.put((byte) i);//写入字节
  }

    //写入一组字节--从bytes中读取数据
  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    if (current == null) {
      getNewInputBuffer();
    }
    int remaining = Math.min(current.remaining(), length);//能读取多少个字节数
    current.put(bytes, offset, remaining);//存储字节内容
    uncompressedBytes += remaining;
    length -= remaining;
    while (length != 0) {//如果进入循环,说明当前磁盘空间不够了,要进行溢出
      spill();
      offset += remaining;//因为已经写入了remaining个字节,因此offset要增加位置
      remaining = Math.min(current.remaining(), length);//继续写入数据
      current.put(bytes, offset, remaining);
      uncompressedBytes += remaining;
      length -= remaining;
    }
  }

  //溢出操作
  private void spill() throws java.io.IOException {
    // if there isn't anything in the current buffer, don't spill
    if (current == null ||
        current.position() == (codec == null ? 0 : HEADER_SIZE)) {//说明没有数据
      return;
    }
    flip();//切换位置到数据的开始位置
    if (codec == null) {
      receiver.output(current);//不压缩则直接把数据写入到底层
      getNewInputBuffer();
    } else {
      if (compressed == null) {
        compressed = getNewOutputBuffer();//创建压缩输出流
      } else if (overflow == null) {
        overflow = getNewOutputBuffer();
      }
      int sizePosn = compressed.position();//此时压缩流的位置
      compressed.position(compressed.position() + HEADER_SIZE);//追加头文件位置
      if (codec.compress(current, compressed, overflow)) {//将current中未压缩的数据进行压缩
        uncompressedBytes = 0;
        // move position back to after the header
        current.position(HEADER_SIZE);
        current.limit(current.capacity());
        // find the total bytes in the chunk
        int totalBytes = compressed.position() - sizePosn - HEADER_SIZE;//压缩后的总大小---即本次压缩后增加了多少个字节的数据位置
        if (overflow != null) {
          totalBytes += overflow.position();//追加压缩后的大小
        }
        compressedBytes += totalBytes + HEADER_SIZE;//压缩后产生的总大小,即数据大小+头文件大小
        writeHeader(compressed, sizePosn, totalBytes, false);//写入头文件,即这段产生了多少个数据字节,
        // if we have less than the next header left, spill it.
        if (compressed.remaining() < HEADER_SIZE) {
          compressed.flip();
          receiver.output(compressed);//将压缩的内容写入到底层文件中
          compressed = overflow;//切换压缩文件
          overflow = null;
        }
      } else {//说明不适合压缩
        compressedBytes += uncompressedBytes + HEADER_SIZE;
        uncompressedBytes = 0;
        // we are using the original, but need to spill the current
        // compressed buffer first. So back up to where we started,
        // flip it and add it to done.
        if (sizePosn != 0) {//如果已经存在压缩数据了,不能让压缩数据和未压缩数据一起混合使用,因次先将压缩的数据写入到底层文件系统
          compressed.position(sizePosn);//设置当前位置
          compressed.flip();
          receiver.output(compressed);//已经压缩的文件要先写入到底层文件系统
          compressed = null;
          // if we have an overflow, clear it and make it the new compress
          // buffer
          if (overflow != null) {//切换overflow变成compressed,因此切换前要清空overflow,理论上overflow上面应该也没有任何数据
            overflow.clear();
            compressed = overflow;
            overflow = null;
          }
        } else {
          compressed.clear();
          if (overflow != null) {
            overflow.clear();
          }
        }

        // now add the current buffer into the done list and get a new one.
        current.position(0);
        // update the header with the current length
        writeHeader(current, 0, current.limit() - HEADER_SIZE, true);
        receiver.output(current);//未压缩文件直接写入底层文件系统
        getNewInputBuffer();
      }
    }
  }

    //记录此时未压缩的文件和压缩文件数量
  @Override
  public void getPosition(PositionRecorder recorder) throws IOException {
    if (codec == null) {
      recorder.addPosition(uncompressedBytes);
    } else {
      recorder.addPosition(compressedBytes);
      recorder.addPosition(uncompressedBytes);
    }
  }

  @Override
  public void flush() throws IOException {
    spill();
    if (compressed != null && compressed.position() != 0) {//将compressed内剩余的数据写入到磁盘里面
      compressed.flip();
      receiver.output(compressed);
    }
    compressed = null;
    uncompressedBytes = 0;
    compressedBytes = 0;
    overflow = null;
    current = null;
  }

  @Override
  public String toString() {
    return name;
  }

    //获取缓冲区大小
  @Override
  public long getBufferSize() {
    if (codec == null) {
      return uncompressedBytes + (current == null ? 0 : current.remaining());//未压缩的文件+剩余存储未压缩字节的空间
    } else {//说明支持压缩
      long result = 0;
      if (current != null) {
        result += current.capacity();
      }
      if (compressed != null) {
        result += compressed.capacity();
      }
      if (overflow != null) {
        result += overflow.capacity();
      }
      return result + compressedBytes;
    }
  }

  /**
   * Set suppress flag
   * 清空缓存的数据
   */
  public void suppress() {
    receiver.suppress();
  }
}

