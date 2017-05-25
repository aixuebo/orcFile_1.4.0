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

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import org.apache.orc.CompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * 压缩/解压缩数据
 */
public class AircompressorCodec implements CompressionCodec {
  private final Compressor compressor;//压缩
  private final Decompressor decompressor;//解压缩

  AircompressorCodec(Compressor compressor, Decompressor decompressor) {
    this.compressor = compressor;
    this.decompressor = decompressor;
  }

    //本地绑定的内存缓冲区
  // Thread local buffer
  private static final ThreadLocal<byte[]> threadBuffer =
      new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
          return null;
        }
      };

    //创建/获取缓冲区
  protected static byte[] getBuffer(int size) {
    byte[] result = threadBuffer.get();
    if (result == null || result.length < size || result.length > size * 2) {
      result = new byte[size];
      threadBuffer.set(result);
    }
    return result;
  }

    /**
     * 压缩
     * @param in the bytes to compress 等待压缩的内容
     * @param out the uncompressed bytes 压缩后向哪里写入
     * @param overflow put any additional bytes here 多余的字节写入到这里面
     * @return true表示可以压缩,false表示不需要压缩,因为压缩后没有减少字节
     * @throws IOException
     */
  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out,
                          ByteBuffer overflow) throws IOException {
    int inBytes = in.remaining();
    // I should work on a patch for Snappy to support an overflow buffer
    // to prevent the extra buffer copy.
    byte[] compressed = getBuffer(compressor.maxCompressedLength(inBytes));//将原始文件inBytes个字节,压缩后产生最多多少个字节,因此产生字节数组存储压缩后的内容
    int outBytes =
        compressor.compress(in.array(), in.arrayOffset() + in.position(), inBytes,
            compressed, 0, compressed.length);//压缩
    if (outBytes < inBytes) {//输入源>压缩后的字节数量,因此说明压缩有效果
      int remaining = out.remaining();//剩余字节数
      if (remaining >= outBytes) {//说明可以容纳压缩后的内容
        System.arraycopy(compressed, 0, out.array(), out.arrayOffset() +
            out.position(), outBytes);//复制
        out.position(out.position() + outBytes);//移动位置
      } else {
        System.arraycopy(compressed, 0, out.array(), out.arrayOffset() +
            out.position(), remaining);//说明先存储remaining个字节
        out.position(out.limit());//out的位置设置到最后
        System.arraycopy(compressed, remaining, overflow.array(),
            overflow.arrayOffset(), outBytes - remaining);//将剩余的字节copy到overflow中
        overflow.position(outBytes - remaining);//即写入了多少个字节
      }
      return true;
    } else {
      return false;
    }
  }

    /**
     * 解压缩
     * @param in the bytes to decompress 输入源
     * @param out the decompressed bytes 输出
     * @throws IOException
     */
  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {
    int inOffset = in.position();
    int uncompressLen =
        decompressor.decompress(in.array(), in.arrayOffset() + inOffset,
        in.limit() - inOffset, out.array(), out.arrayOffset() + out.position(),
            out.remaining());//解压缩
    out.position(uncompressLen + out.position());
    out.flip();
  }

  @Override
  public CompressionCodec modify(EnumSet<Modifier> modifiers) {
    // snappy allows no modifications
    return this;
  }

  @Override
  public void reset() {
    // Nothing to do.
  }

  @Override
  public void close() {
    // Nothing to do.
  }
}
