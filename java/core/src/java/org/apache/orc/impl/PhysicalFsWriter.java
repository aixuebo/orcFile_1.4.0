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
    //key排序,先按照data还是index分组排序,然后按照列的序号排序,然后是kind排序
  private final Map<StreamName, BufferedStream> streams = new TreeMap<>();//每一个name对应一个缓冲流

  private long adjustedStripeSize;
  private long headerLength;//orc的头文件长度
  private long stripeStart;
  private int metadataLength;//元数据字节长度
  private int footerLength;//记录footer一共多少个字节

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

      //将数据进行codec压缩,然后压缩后的结果写入rawWriter,即写入HDFS
    writer = new OutStream("metadata", bufferSize, codec,
        new DirectStream(rawWriter));//创建一个可能是压缩的流,因为codec是压缩方式
    protobufWriter = CodedOutputStream.newInstance(writer);//真正的输出流,可能会倍压缩
  }

  @Override
  public CompressionCodec getCompressionCodec() {
    return codec;
  }

    //参数是当前stripe需要的三部分数据大小
  private void padStripe(long indexSize, long dataSize, int footerSize) throws IOException {
    this.stripeStart = rawWriter.getPos();
    final long currentStripeSize = indexSize + dataSize + footerSize;//当前stripe需要的总字节
    final long available = blockSize - (stripeStart % blockSize);//可用字节数  stripeStart % blockSize 表示余数,即当前位置是整数数据块倍数后,还多写了多少个数据字节,因此用blockSize-他,就是还可以写入多少个字节
    final long overflow = currentStripeSize - adjustedStripeSize;
    final float availRatio = (float) available / (float) defaultStripeSize;

    if (availRatio > 0.0f && availRatio < 1.0f //表示当前stripe的信息可以放在一个默认的defaultStripeSize大小里面
        && availRatio > paddingTolerance) {//但是比例有些大,需要调整
      // adjust default stripe size to fit into remaining space,
        //调整默认的stripe大小去适应剩余的空间
      //also adjust the next stripe for correction based on the current stripe size
       //也调整下一个stripe大小 基于当前stripe的大小进行更正
      // and user specified padding tolerance. Since stripe size can overflow
      // the default stripe size we should apply this correction to avoid
      // writing portion of last stripe to next hdfs block.我们应该更正,避免写入剩余的一部分自己写入到下一个数据块了
      double correction = overflow > 0 ? (double) overflow
          / (double) adjustedStripeSize : 0.0;//没有溢出,则0,有溢出,则溢出的字节/调整的字节

      // correction should not be greater than user specified padding
      // tolerance
      correction = correction > paddingTolerance ? paddingTolerance
          : correction;

      // adjust next stripe size based on current stripe estimate correction
      adjustedStripeSize = (long) ((1.0f - correction) * (availRatio * defaultStripeSize));
    } else if (availRatio >= 1.0) {//说明当前stripe的信息比默认的defaultStripeSize要大很多
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
        rawWriter.write(pad, 0, writeLen);//写入空的字节,用于占位,让该数据块结束
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

    //写入stripe的footer信息
  private void writeStripeFooter(OrcProto.StripeFooter footer,
                                 long dataSize,
                                 long indexSize,
                                 OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    footer.writeTo(protobufWriter);//压缩算法写入
    protobufWriter.flush();
    writer.flush();
    dirEntry.setOffset(stripeStart);//记录该stripe的开始位置
    dirEntry.setFooterLength(rawWriter.getPos() - stripeStart - dataSize - indexSize);//记录footer的长度
  }

    //将参数对应的元数据写入到rawWriter流中,可能有压缩的方式写入的
  @Override
  public void writeFileMetadata(OrcProto.Metadata.Builder builder) throws IOException {
    long startPosn = rawWriter.getPos();//写入前位置
      //真正的写入操作
    OrcProto.Metadata metadata = builder.build();
    metadata.writeTo(protobufWriter);//压缩算法写入
    protobufWriter.flush();
    writer.flush();
    this.metadataLength = (int) (rawWriter.getPos() - startPosn);//记录写入后的位置-写入前的位置,即元数据多少个字节
  }

    //将Footer写出到rawWriter流中
  @Override
  public void writeFileFooter(OrcProto.Footer.Builder builder) throws IOException {
    long bodyLength = rawWriter.getPos() - metadataLength;//获取非元数据的字节数量,即body的字节内容长度
      //设置Footer内容
    builder.setContentLength(bodyLength);
    builder.setHeaderLength(headerLength);
    long startPosn = rawWriter.getPos();
    OrcProto.Footer footer = builder.build();
    footer.writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    this.footerLength = (int) (rawWriter.getPos() - startPosn);//记录footer一共多少个字节
  }

    //写入PostScript对象
  @Override
  public long writePostScript(OrcProto.PostScript.Builder builder) throws IOException {
      //设置PostScript对象
    builder.setFooterLength(footerLength);
    builder.setMetadataLength(metadataLength);
    OrcProto.PostScript ps = builder.build();
    // need to write this uncompressed
    long startPosn = rawWriter.getPos();
    ps.writeTo(rawWriter);//写入PostScript对象 ---不需要压缩,因为pb已经压缩完了
    long length = rawWriter.getPos() - startPosn;//计算PostScript对象占用字节数
    if (length > 255) {
      throw new IllegalArgumentException("PostScript too large at " + length);
    }
    rawWriter.writeByte((int)length);//写入一个字节,表示PostScript对象占用字节数
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

  //将stripe的内容都写在buffer中了,将其内容写入输出流中
  @Override
  public void appendRawStripe(ByteBuffer buffer,
      OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    long start = rawWriter.getPos();
    int length = buffer.remaining();
    long availBlockSpace = blockSize - (start % blockSize);//找到可用数据块的大小

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
        length);//将buffer内容写入到rawWriter中--不需要压缩.因为可能已经压缩完成了
    dirEntry.setOffset(start);//只是设置开始位置,没办法设置index、data、footer的字节大小
  }


  /**
   * This class is used to hold the contents of streams as they are buffered.
   * The TreeWriters write to the outStream and the codec compresses the
   * data as buffers fill up and stores them in the output list. When the
   * stripe is being written, the whole stream is written to the file.
   * 内部先缓存一下
   */
  private static final class BufferedStream implements OutputReceiver {
    private boolean isSuppressed = false;//true表示抑制,即取消一部分数据
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
      output.clear();//取消内部缓存的数据
    }

    /**
     * Write any saved buffers to the OutputStream if needed, and clears all the
     * buffers.
     * 将缓存的数据写入到参数对应的输出流中
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

    /**
     * 表示一个stripe完成了,要将该数据写入到磁盘上
     * @param footerBuilder
     * @param dirEntry File metadata entry for the stripe, to be updated with 更新一个stripe的元数据,包含索引大小、数据大小
     * @throws IOException
     */
  @Override
  public void finalizeStripe(OrcProto.StripeFooter.Builder footerBuilder,
                             OrcProto.StripeInformation.Builder dirEntry
                             ) throws IOException {
    long indexSize = 0;//索引占据大小
    long dataSize = 0;//数据占据大小
     //循环所有的流
    for (Map.Entry<StreamName, BufferedStream> pair: streams.entrySet()) {
      BufferedStream receiver = pair.getValue();//获取具体的缓冲流
      if (!receiver.isSuppressed) {//说明要写入数据---一旦是true,就不写入数据
        long streamSize = receiver.getOutputSize();//缓冲区大小
        StreamName name = pair.getKey();
        footerBuilder.addStreams(OrcProto.Stream.newBuilder().setColumn(name.getColumn())
            .setKind(name.getKind()).setLength(streamSize));//添加该列--什么方式的数据--占用多少字节长度
        if (StreamName.Area.INDEX == name.getArea()) {//根据类型,判断数据增加大小还是索引增加大小
          indexSize += streamSize;
        } else {
          dataSize += streamSize;
        }
      }
    }
    dirEntry.setIndexLength(indexSize).setDataLength(dataSize);//设置该stripe的索引大小以及数据大小

    OrcProto.StripeFooter footer = footerBuilder.build();
    // Do we need to pad the file so the stripe doesn't straddle a block boundary?
    padStripe(indexSize, dataSize, footer.getSerializedSize());

    // write out the data streams
    for (Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
      pair.getValue().spillToDiskAndClear(rawWriter);//将每一个Stream的缓冲区数据写入到输出流中
    }
    // Write out the footer.写入stripe的footer信息
    writeStripeFooter(footer, dataSize, indexSize, dirEntry);
  }

    //先写入文件头
  @Override
  public void writeHeader() throws IOException {
    rawWriter.writeBytes(OrcFile.MAGIC);
    headerLength = rawWriter.getPos();
  }

    //获取name对应的缓冲流
  @Override
  public BufferedStream createDataStream(StreamName name) {
    BufferedStream result = streams.get(name);
    if (result == null) {
      result = new BufferedStream();
      streams.put(name, result);
    }
    return result;
  }

    //写入index索引
  @Override
  public void writeIndex(StreamName name,
                         OrcProto.RowIndex.Builder index,
                         CompressionCodec codec) throws IOException {
    OutputStream stream = new OutStream(path.toString(), bufferSize, codec,
        createDataStream(name));
    index.build().writeTo(stream);//将index的内容写入到stream缓冲池中
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
