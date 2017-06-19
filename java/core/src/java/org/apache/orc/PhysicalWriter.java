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

import org.apache.orc.impl.StreamName;

/**
 * This interface separates the physical layout of ORC files from the higher
 * level details.
 *
 * This API is limited to being used by LLAP.
 */
public interface PhysicalWriter {

  /**
   * The target of an output stream.
   * 目标的一个输出流
   */
  interface OutputReceiver {
    /**
     * Output the given buffer to the final destination
     * 将参数字节数组输出到目标out中
     * @param buffer the buffer to output
     * @throws IOException
     */
    void output(ByteBuffer buffer) throws IOException;

    /**
     * Suppress this stream from being written to the stripe.
     * 止住这个流---即如何还原已经添加进去的流
     */
    void suppress();
  }
  /**
   * Writes the header of the file, which consists of the magic "ORC" bytes.
   * @throws IOException
   * 写入文件头数据,比如orc的魔
   */
  void writeHeader() throws IOException;

  /**
   * Create an OutputReceiver for the given name.
   * @param name the name of the stream
   * @throws IOException
   * 创建一个本地输出流
   */
  OutputReceiver createDataStream(StreamName name) throws IOException;

  /**
   * Write an index in the given stream name.
   * @param name the name of the stream
   * @param index the bloom filter to write
   * @param codec the compression codec to use
   */
  void writeIndex(StreamName name,
                  OrcProto.RowIndex.Builder index,
                  CompressionCodec codec) throws IOException;

  /**
   * Write a bloom filter index in the given stream name.
   * @param name the name of the stream
   * @param bloom the bloom filter to write
   * @param codec the compression codec to use
   */
  void writeBloomFilter(StreamName name,
                        OrcProto.BloomFilterIndex.Builder bloom,
                        CompressionCodec codec) throws IOException;

  /**
   * Flushes the data in all the streams, spills them to disk, write out stripe
   * footer.
   * 写入一个完整的stripe
   * @param footer Stripe footer to be updated with relevant data and written out.
   * @param dirEntry File metadata entry for the stripe, to be updated with
   *                 relevant data.
   */
  void finalizeStripe(OrcProto.StripeFooter.Builder footer,
                      OrcProto.StripeInformation.Builder dirEntry) throws IOException;

  /**
   * Writes out the file metadata.
   * 写入元数据
   * @param builder Metadata builder to finalize and write.
   */
  void writeFileMetadata(OrcProto.Metadata.Builder builder) throws IOException;

  /**
   * Writes out the file footer.
   * 写入footer
   * @param builder Footer builder to finalize and write.
   */
  void writeFileFooter(OrcProto.Footer.Builder builder) throws IOException;

  /**
   * Writes out the postscript (including the size byte if needed).
   * 写入postScript
   * @param builder Postscript builder to finalize and write.
   */
  long writePostScript(OrcProto.PostScript.Builder builder) throws IOException;

  /**
   * Closes the writer.
   */
  void close() throws IOException;

  /**
   * Flushes the writer so that readers can see the preceeding postscripts.
   */
  void flush() throws IOException;

  /**
   * Appends raw stripe data (e.g. for file merger).
   * 追加一个原生的一个stripe字节数组,可能用于merge合并操作
   * @param stripe Stripe data buffer.
   * @param dirEntry File metadata entry for the stripe, to be updated with
   *                 relevant data.
   * @throws IOException
   */
  void appendRawStripe(ByteBuffer stripe,
                       OrcProto.StripeInformation.Builder dirEntry
                       ) throws IOException;

  /** Gets a compression codec used by this writer. */
  CompressionCodec getCompressionCodec();
}
