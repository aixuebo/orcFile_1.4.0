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
package org.apache.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

public interface CompressionCodec {

    //通过给定枚举类型去选择合适的压缩算法
  enum Modifier {
    /* speed/compression tradeoffs 速度上的控制*/
    FASTEST,
    FAST,
    DEFAULT,
    /* data sensitivity modifiers 数据上的控制*/
    TEXT,
    BINARY
  };

  /**
   * 压缩
   * Compress the in buffer to the out buffer.
   * @param in the bytes to compress 输入内容
   * @param out the uncompressed bytes 压缩后的输出
   * @param overflow put any additional bytes here 如果压缩后的输出out装不下内容,则向overflow内继续输出压缩后内容
   * @return true if the output is smaller than input
   * @throws IOException
   */
  boolean compress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow
                  ) throws IOException;

  /**
   * 解压缩
   * Decompress the in buffer to the out buffer.
   * @param in the bytes to decompress 压缩文件
   * @param out the decompressed bytes 解压缩后输出的文件
   * @throws IOException
   */
  void decompress(ByteBuffer in, ByteBuffer out) throws IOException;

  /**
   * Produce a modified compression codec if the underlying algorithm allows
   * modification.
   *
   * This does not modify the current object, but returns a new object if
   * modifications are possible. Returns the same object if no modifications
   * are possible.
   * @param modifiers compression modifiers (nullable)
   * @return codec for use after optional modification
   * 通过给定枚举类型去选择合适的压缩算法
   */
  CompressionCodec modify(EnumSet<Modifier> modifiers);

  /** Resets the codec, preparing it for reuse. */
  void reset();

  /** Closes the codec, releasing the resources. */
  void close();
}
