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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PhysicalFsWriter implements PhysicalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PhysicalFsWriter.class);

  private static final int HDFS_BUFFER_SIZE = 256 * 1024;

  private final FSDataOutputStream rawWriter;//文件系统的输出流
  // the compressed metadata information outStream
  private OutStream writer = null;//对rawWriter又进一步包装,里面包含额外的信息输出,比如元数据等
  // a protobuf outStream around streamFactory
  private CodedOutputStream protobufWriter = null;

  private final Path path;//存储路径
  private final long blockSize;//数据块大小
  private final int bufferSize;//缓冲区
  private final double paddingTolerance;
  private final long defaultStripeSize;
  private final CompressionKind compress;//压缩算法
  private final CompressionCodec codec;//压缩对象,根据压缩算法创建相应的压缩对象
  private final boolean addBlockPadding;

  // the streams that make up the current stripe
  private final Map<StreamName, BufferedStream> streams =
    new TreeMap<>();

  private long adjustedStripeSize;
  private long headerLength;//orc的头文件长度
  private long stripeStart;
  private int metadataLength;
  private int footerLength;

  public PhysicalFsWriter(FileSystem fs,
                          Path path,
                          OrcFile.WriterOptions opts) throws IOException {
    this.path = path;
    this.defaultStripeSize = this.adjustedStripeSize = opts.getStripeSize();
    this.addBlockPadding = opts.getBlockPadding();
    if (opts.isEnforceBufferSize()) {
      this.bufferSize = opts.getBufferSize();
    } else {
      this.bufferSize = WriterImpl.getEstimatedBufferSize(defaultStripeSize,
          opts.getSchema().getMaximumId() + 1,
          opts.getBufferSize());
    }
    this.compress = opts.getCompress();//压缩算法
    this.paddingTolerance = opts.getPaddingTolerance();
    this.blockSize = opts.getBlockSize();
    LOG.info("ORC writer created for path: {} with stripeSize: {} blockSize: {}" +
        " compression: {} bufferSize: {}", path, defaultStripeSize, blockSize,
        compress, bufferSize);
    rawWriter = fs.create(path, false, HDFS_BUFFER_SIZE,
        fs.getDefaultReplication(path), blockSize);//创建文件系统的输出流
    codec = OrcCodecPool.getCodec(compress);//根据压缩算法创建压缩对象
    writer = new OutStream("metadata", bufferSize, codec,
        new DirectStream(rawWriter));
    protobufWriter = CodedOutputStream.newInstance(writer);
  }

  @Override
  public CompressionCodec getCompressionCodec() {
    return codec;
  }

  private void padStripe(long indexSize, long dataSize, int footerSize) throws IOException {
    this.stripeStart = rawWriter.getPos();
    final long currentStripeSize = indexSize + dataSize + footerSize;
    final long available = blockSize - (stripeStart % blockSize);
    final long overflow = currentStripeSize - adjustedStripeSize;
    final float availRatio = (float) available / (float) defaultStripeSize;

    if (availRatio > 0.0f && availRatio < 1.0f
        && availRatio > paddingTolerance) {
      // adjust default stripe size to fit into remaining space, also adjust
      // the next stripe for correction based on the current stripe size
      // and user specified padding tolerance. Since stripe size can overflow
      // the default stripe size we should apply this correction to avoid
      // writing portion of last stripe to next hdfs block.
      double correction = overflow > 0 ? (double) overflow
          / (double) adjustedStripeSize : 0.0;

      // correction should not be greater than user specified padding
      // tolerance
      correction = correction > paddingTolerance ? paddingTolerance
          : correction;

      // adjust next stripe size based on current stripe estimate correction
      adjustedStripeSize = (long) ((1.0f - correction) * (availRatio * defaultStripeSize));
    } else if (availRatio >= 1.0) {
      adjustedStripeSize = defaultStripeSize;
    }

    if (availRatio < paddingTolerance && addBlockPadding) {
      long padding = blockSize - (stripeStart % blockSize);
      byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, padding)];
      LOG.info(String.format("Padding ORC by %d bytes (<=  %.2f * %d)",
          padding, availRatio, defaultStripeSize));
      stripeStart += padding;
      while (padding > 0) {
        int writeLen = (int) Math.min(padding, pad.length);
        rawWriter.write(pad, 0, writeLen);
        padding -= writeLen;
      }
      adjustedStripeSize = defaultStripeSize;
    } else if (currentStripeSize < blockSize
        && (stripeStart % blockSize) + currentStripeSize > blockSize) {
      // even if you don't pad, reset the default stripe size when crossing a
      // block boundary
      adjustedStripeSize = defaultStripeSize;
    }
  }

  /**
   * An output receiver that writes the ByteBuffers to the output stream
   * as they are received.
   * 不用任何包装容器,直接将数据写入到底层的输出中
   */
  private static class DirectStream implements OutputReceiver {
    private final FSDataOutputStream output;//对应目标的输出流

    DirectStream(FSDataOutputStream output) {
      this.output = output;
    }

      //将buffer的内容写入到output中
    @Override
    public void output(ByteBuffer buffer) throws IOException {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }

    @Override
    public void suppress() {
      throw new UnsupportedOperationException("Can't suppress direct stream");
    }
  }

  private void writeStripeFooter(OrcProto.StripeFooter footer,
                                 long dataSize,
                                 long indexSize,
                                 OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    footer.writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    dirEntry.setOffset(stripeStart);
    dirEntry.setFooterLength(rawWriter.getPos() - stripeStart - dataSize - indexSize);
  }

  @Override
  public void writeFileMetadata(OrcProto.Metadata.Builder builder) throws IOException {
    long startPosn = rawWriter.getPos();
    OrcProto.Metadata metadata = builder.build();
    metadata.writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    this.metadataLength = (int) (rawWriter.getPos() - startPosn);
  }

  @Override
  public void writeFileFooter(OrcProto.Footer.Builder builder) throws IOException {
    long bodyLength = rawWriter.getPos() - metadataLength;
    builder.setContentLength(bodyLength);
    builder.setHeaderLength(headerLength);
    long startPosn = rawWriter.getPos();
    OrcProto.Footer footer = builder.build();
    footer.writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    this.footerLength = (int) (rawWriter.getPos() - startPosn);
  }

  @Override
  public long writePostScript(OrcProto.PostScript.Builder builder) throws IOException {
    builder.setFooterLength(footerLength);
    builder.setMetadataLength(metadataLength);
    OrcProto.PostScript ps = builder.build();
    // need to write this uncompressed
    long startPosn = rawWriter.getPos();
    ps.writeTo(rawWriter);
    long length = rawWriter.getPos() - startPosn;
    if (length > 255) {
      throw new IllegalArgumentException("PostScript too large at " + length);
    }
    rawWriter.writeByte((int)length);
    return rawWriter.getPos();
  }

  @Override
  public void close() throws IOException {
    OrcCodecPool.returnCodec(compress, codec);
    rawWriter.close();
  }

  @Override
  public void flush() throws IOException {
    rawWriter.hflush();
  }

  @Override
  public void appendRawStripe(ByteBuffer buffer,
      OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    long start = rawWriter.getPos();
    int length = buffer.remaining();
    long availBlockSpace = blockSize - (start % blockSize);

    // see if stripe can fit in the current hdfs block, else pad the remaining
    // space in the block
    if (length < blockSize && length > availBlockSpace &&
        addBlockPadding) {
      byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, availBlockSpace)];
      LOG.info(String.format("Padding ORC by %d bytes while merging..",
          availBlockSpace));
      start += availBlockSpace;
      while (availBlockSpace > 0) {
        int writeLen = (int) Math.min(availBlockSpace, pad.length);
        rawWriter.write(pad, 0, writeLen);
        availBlockSpace -= writeLen;
      }
    }
    rawWriter.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
        length);
    dirEntry.setOffset(start);
  }


  /**
   * This class is used to hold the contents of streams as they are buffered.
   * The TreeWriters write to the outStream and the codec compresses the
   * data as buffers fill up and stores them in the output list. When the
   * stripe is being written, the whole stream is written to the file.
   * 内部先缓存一下
   */
  private static final class BufferedStream implements OutputReceiver {
    private boolean isSuppressed = false;
    private final List<ByteBuffer> output = new ArrayList<>();

      //将该写入的流先缓存起来
    @Override
    public void output(ByteBuffer buffer) {
      if (!isSuppressed) {
        output.add(buffer);
      }
    }

      //清空out缓存队列
    public void suppress() {
      isSuppressed = true;
      output.clear();
    }

    /**
     * Write any saved buffers to the OutputStream if needed, and clears all the
     * buffers.
     */
    void spillToDiskAndClear(FSDataOutputStream raw
                                       ) throws IOException {
      if (!isSuppressed) {//false表示将所有缓存的数据写入到磁盘中
        for (ByteBuffer buffer: output) {
          raw.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
            buffer.remaining());
        }
        output.clear();
      }
      isSuppressed = false;
    }

    /**
     * Get the number of bytes that will be written to the output.
     *
     * Assumes the stream writing into this receiver has already been flushed.
     * @return number of bytes
     * 获取缓存了多少个字节
     */
    public long getOutputSize() {
      long result = 0;
      for (ByteBuffer buffer: output) {
        result += buffer.remaining();//每一个缓存还有多少数据可以读
      }
      return result;
    }
  }

  @Override
  public void finalizeStripe(OrcProto.StripeFooter.Builder footerBuilder,
                             OrcProto.StripeInformation.Builder dirEntry
                             ) throws IOException {
    long indexSize = 0;
    long dataSize = 0;
    for (Map.Entry<StreamName, BufferedStream> pair: streams.entrySet()) {
      BufferedStream receiver = pair.getValue();
      if (!receiver.isSuppressed) {
        long streamSize = receiver.getOutputSize();
        StreamName name = pair.getKey();
        footerBuilder.addStreams(OrcProto.Stream.newBuilder().setColumn(name.getColumn())
            .setKind(name.getKind()).setLength(streamSize));
        if (StreamName.Area.INDEX == name.getArea()) {
          indexSize += streamSize;
        } else {
          dataSize += streamSize;
        }
      }
    }
    dirEntry.setIndexLength(indexSize).setDataLength(dataSize);

    OrcProto.StripeFooter footer = footerBuilder.build();
    // Do we need to pad the file so the stripe doesn't straddle a block boundary?
    padStripe(indexSize, dataSize, footer.getSerializedSize());

    // write out the data streams
    for (Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
      pair.getValue().spillToDiskAndClear(rawWriter);
    }
    // Write out the footer.
    writeStripeFooter(footer, dataSize, indexSize, dirEntry);
  }

  @Override
  public void writeHeader() throws IOException {
    rawWriter.writeBytes(OrcFile.MAGIC);
    headerLength = rawWriter.getPos();
  }

  @Override
  public BufferedStream createDataStream(StreamName name) {
    BufferedStream result = streams.get(name);
    if (result == null) {
      result = new BufferedStream();
      streams.put(name, result);
    }
    return result;
  }

  @Override
  public void writeIndex(StreamName name,
                         OrcProto.RowIndex.Builder index,
                         CompressionCodec codec) throws IOException {
    OutputStream stream = new OutStream(path.toString(), bufferSize, codec,
        createDataStream(name));
    index.build().writeTo(stream);
    stream.flush();
  }

  @Override
  public void writeBloomFilter(StreamName name,
                               OrcProto.BloomFilterIndex.Builder bloom,
                               CompressionCodec codec) throws IOException {
    OutputStream stream = new OutStream(path.toString(), bufferSize, codec,
        createDataStream(name));
    bloom.build().writeTo(stream);
    stream.flush();
  }

  @Override
  public String toString() {
    return path.toString();
  }
}
