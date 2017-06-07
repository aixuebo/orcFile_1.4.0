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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.orc.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.io.DiskRange;

import com.google.protobuf.CodedInputStream;

public abstract class InStream extends InputStream {

  private static final Logger LOG = LoggerFactory.getLogger(InStream.class);
  public static final int PROTOBUF_MESSAGE_MAX_LIMIT = 1024 << 20; // 1GB

  protected final String name;
  protected long length;//要读取多少个字节,即总长度

  public InStream(String name, long length) {
    this.name = name;
    this.length = length;
  }

  public String getStreamName() {
    return name;
  }

  public long getStreamLength() {
    return length;
  }

  @Override
  public abstract void close();

  //不需要解压缩的读取流方式
  public static class UncompressedStream extends InStream {
    private List<DiskRange> bytes;//数据原始内容
    private long length;//总长度
    protected long currentOffset;//当前读取到第几个字节了
    private ByteBuffer range;//缓冲区
    private int currentRange;//当前处理第几个DiskRange

    public UncompressedStream(String name, List<DiskRange> input, long length) {
      super(name, length);
      reset(input, length);
    }

    protected void reset(List<DiskRange> input, long length) {
      this.bytes = input;
      this.length = length;
      currentRange = 0;
      currentOffset = 0;
      range = null;
    }

    //从缓冲区中读取一个字节
    @Override
    public int read() {
      if (range == null || range.remaining() == 0) {//说明range没有数据了
        if (currentOffset == length) {//说明已经读取全部数据了
          return -1;
        }
        seek(currentOffset);
      }
      currentOffset += 1;
      return 0xff & range.get();//从range中读取一个字节
    }

      //读取length字节,读取到data数组中,返回真的读取了多少个字节
    @Override
    public int read(byte[] data, int offset, int length) {
      if (range == null || range.remaining() == 0) {//说明缓冲区内没有数据了
        if (currentOffset == this.length) {
          return -1;
        }
        seek(currentOffset);
      }
      int actualLength = Math.min(length, range.remaining());//从缓冲区中读取数据
      range.get(data, offset, actualLength);
      currentOffset += actualLength;
      return actualLength;
    }

    @Override
    public int available() {
      if (range != null && range.remaining() > 0) {//缓冲区剩余多少字节
        return range.remaining();
      }
      return (int) (length - currentOffset);
    }

    @Override
    public void close() {
      currentRange = bytes.size();
      currentOffset = length;
      // explicit de-ref of bytes[]
      bytes.clear();
    }

      //定位到下一个位置
    @Override
    public void seek(PositionProvider index) throws IOException {
      seek(index.getNext());
    }

      //定位到desired位置
    public void seek(long desired) {
      if (desired == 0 && bytes.isEmpty()) {
        return;
      }
      int i = 0;
      for (DiskRange curRange : bytes) {//循环每一个数据源
        if (curRange.getOffset() <= desired &&
            (desired - curRange.getOffset()) < curRange.getLength()) {//说明要查找的位置在该DiskRange里面
          currentOffset = desired;
          currentRange = i;
          this.range = curRange.getData().duplicate();
          int pos = range.position();
          pos += (int)(desired - curRange.getOffset()); // this is why we duplicate 移动到参数的位置上
          this.range.position(pos);
          return;
        }
        ++i;
      }
      // if they are seeking to the precise end, go ahead and let them go there
      int segments = bytes.size();
      if (segments != 0 && desired == bytes.get(segments - 1).getEnd()) {
        currentOffset = desired;
        currentRange = segments - 1;
        DiskRange curRange = bytes.get(currentRange);
        this.range = curRange.getData().duplicate();
        int pos = range.position();
        pos += (int)(desired - curRange.getOffset()); // this is why we duplicate
        this.range.position(pos);
        return;
      }
      throw new IllegalArgumentException("Seek in " + name + " to " +
        desired + " is outside of the data");
    }

    @Override
    public String toString() {
      return "uncompressed stream " + name + " position: " + currentOffset +
          " length: " + length + " range: " + currentRange +
          " offset: " + (range == null ? 0 : range.position()) + " limit: " + (range == null ? 0 : range.limit());
    }
  }

  private static ByteBuffer allocateBuffer(int size, boolean isDirect) {
    // TODO: use the same pool as the ORC readers
    if (isDirect) {
      return ByteBuffer.allocateDirect(size);
    } else {
      return ByteBuffer.allocate(size);
    }
  }

  //读取压缩的流
  private static class CompressedStream extends InStream {
    private final List<DiskRange> bytes;
    private final int bufferSize;
    private ByteBuffer uncompressed;
    private final CompressionCodec codec;
    private ByteBuffer compressed;
    private long currentOffset;
    private int currentRange;
    private boolean isUncompressedOriginal;

    public CompressedStream(String name, List<DiskRange> input, long length,
                            CompressionCodec codec, int bufferSize) {
      super(name, length);
      this.bytes = input;
      this.codec = codec;
      this.bufferSize = bufferSize;
      currentOffset = 0;
      currentRange = 0;
    }

    private void allocateForUncompressed(int size, boolean isDirect) {
      uncompressed = allocateBuffer(size, isDirect);
    }

    private void readHeader() throws IOException {
      if (compressed == null || compressed.remaining() <= 0) {
        seek(currentOffset);
      }
      if (compressed.remaining() > OutStream.HEADER_SIZE) {
        int b0 = compressed.get() & 0xff;
        int b1 = compressed.get() & 0xff;
        int b2 = compressed.get() & 0xff;
        boolean isOriginal = (b0 & 0x01) == 1;
        int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >> 1);

        if (chunkLength > bufferSize) {
          throw new IllegalArgumentException("Buffer size too small. size = " +
              bufferSize + " needed = " + chunkLength);
        }
        // read 3 bytes, which should be equal to OutStream.HEADER_SIZE always
        assert OutStream.HEADER_SIZE == 3 : "The Orc HEADER_SIZE must be the same in OutStream and InStream";
        currentOffset += OutStream.HEADER_SIZE;

        ByteBuffer slice = this.slice(chunkLength);

        if (isOriginal) {
          uncompressed = slice;
          isUncompressedOriginal = true;
        } else {
          if (isUncompressedOriginal) {
            allocateForUncompressed(bufferSize, slice.isDirect());
            isUncompressedOriginal = false;
          } else if (uncompressed == null) {
            allocateForUncompressed(bufferSize, slice.isDirect());
          } else {
            uncompressed.clear();
          }
          codec.decompress(slice, uncompressed);
         }
      } else {
        throw new IllegalStateException("Can't read header at " + this);
      }
    }

    @Override
    public int read() throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        if (currentOffset == length) {
          return -1;
        }
        readHeader();
      }
      return 0xff & uncompressed.get();
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        if (currentOffset == this.length) {
          return -1;
        }
        readHeader();
      }
      int actualLength = Math.min(length, uncompressed.remaining());
      uncompressed.get(data, offset, actualLength);
      return actualLength;
    }

    @Override
    public int available() throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        if (currentOffset == length) {
          return 0;
        }
        readHeader();
      }
      return uncompressed.remaining();
    }

    @Override
    public void close() {
      uncompressed = null;
      compressed = null;
      currentRange = bytes.size();
      currentOffset = length;
      bytes.clear();
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      seek(index.getNext());
      long uncompressedBytes = index.getNext();
      if (uncompressedBytes != 0) {
        readHeader();
        uncompressed.position(uncompressed.position() +
                              (int) uncompressedBytes);
      } else if (uncompressed != null) {
        // mark the uncompressed buffer as done
        uncompressed.position(uncompressed.limit());
      }
    }

    /* slices a read only contiguous buffer of chunkLength */
    private ByteBuffer slice(int chunkLength) throws IOException {
      int len = chunkLength;
      final long oldOffset = currentOffset;
      ByteBuffer slice;
      if (compressed.remaining() >= len) {
        slice = compressed.slice();
        // simple case
        slice.limit(len);
        currentOffset += len;
        compressed.position(compressed.position() + len);
        return slice;
      } else if (currentRange >= (bytes.size() - 1)) {
        // nothing has been modified yet
        throw new IOException("EOF in " + this + " while trying to read " +
            chunkLength + " bytes");
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format(
            "Crossing into next BufferChunk because compressed only has %d bytes (needs %d)",
            compressed.remaining(), len));
      }

      // we need to consolidate 2 or more buffers into 1
      // first copy out compressed buffers
      ByteBuffer copy = allocateBuffer(chunkLength, compressed.isDirect());
      currentOffset += compressed.remaining();
      len -= compressed.remaining();
      copy.put(compressed);
      ListIterator<DiskRange> iter = bytes.listIterator(currentRange);

      while (len > 0 && iter.hasNext()) {
        ++currentRange;
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Read slow-path, >1 cross block reads with %s", this.toString()));
        }
        DiskRange range = iter.next();
        compressed = range.getData().duplicate();
        if (compressed.remaining() >= len) {
          slice = compressed.slice();
          slice.limit(len);
          copy.put(slice);
          currentOffset += len;
          compressed.position(compressed.position() + len);
          return copy;
        }
        currentOffset += compressed.remaining();
        len -= compressed.remaining();
        copy.put(compressed);
      }

      // restore offsets for exception clarity
      seek(oldOffset);
      throw new IOException("EOF in " + this + " while trying to read " +
          chunkLength + " bytes");
    }

      //定位到参数位置
    private void seek(long desired) throws IOException {
      if (desired == 0 && bytes.isEmpty()) {
        return;
      }
      int i = 0;
      for (DiskRange range : bytes) {
        if (range.getOffset() <= desired && desired < range.getEnd()) {//说明参数的位置在该DiskRange内
          currentRange = i;
          compressed = range.getData().duplicate();
          int pos = compressed.position();
          pos += (int)(desired - range.getOffset());
          compressed.position(pos);
          currentOffset = desired;
          return;
        }
        ++i;
      }
      // if they are seeking to the precise end, go ahead and let them go there
      int segments = bytes.size();
      if (segments != 0 && desired == bytes.get(segments - 1).getEnd()) {
        DiskRange range = bytes.get(segments - 1);
        currentRange = segments - 1;
        compressed = range.getData().duplicate();
        compressed.position(compressed.limit());
        currentOffset = desired;
        return;
      }
      throw new IOException("Seek outside of data in " + this + " to " + desired);
    }

    private String rangeString() {
      StringBuilder builder = new StringBuilder();
      int i = 0;
      for (DiskRange range : bytes) {
        if (i != 0) {
          builder.append("; ");
        }
        builder.append(" range " + i + " = " + range.getOffset()
            + " to " + (range.getEnd() - range.getOffset()));
        ++i;
      }
      return builder.toString();
    }

    @Override
    public String toString() {
      return "compressed stream " + name + " position: " + currentOffset +
          " length: " + length + " range: " + currentRange +
          " offset: " + (compressed == null ? 0 : compressed.position()) + " limit: " + (compressed == null ? 0 : compressed.limit()) +
          rangeString() +
          (uncompressed == null ? "" :
              " uncompressed: " + uncompressed.position() + " to " +
                  uncompressed.limit());
    }
  }

  public abstract void seek(PositionProvider index) throws IOException;

  /**
   * Create an input stream from a list of buffers.
   * @param streamName the name of the stream
   * @param buffers the list of ranges of bytes for the stream
   * @param offsets a list of offsets (the same length as input) that must
   *                contain the first offset of the each set of bytes in input
   * @param length the length in bytes of the stream
   * @param codec the compression codec
   * @param bufferSize the compression buffer size
   * @return an input stream
   * @throws IOException
   */
  //@VisibleForTesting
  @Deprecated
  public static InStream create(String streamName,
                                ByteBuffer[] buffers,
                                long[] offsets,//与ByteBuffer数组相对应,提供offset位置
                                long length,
                                CompressionCodec codec,
                                int bufferSize) throws IOException {
    List<DiskRange> input = new ArrayList<DiskRange>(buffers.length);
    for (int i = 0; i < buffers.length; ++i) {//将ByteBuffer数组转换成DiskRange对象
      input.add(new BufferChunk(buffers[i], offsets[i]));
    }
    return create(streamName, input, length, codec, bufferSize);
  }

  /**
   * Create an input stream from a list of disk ranges with data.
   * @param name the name of the stream
   * @param input the list of ranges of bytes for the stream; from disk or cache
   * @param length the length in bytes of the stream
   * @param codec the compression codec
   * @param bufferSize the compression buffer size
   * @return an input stream
   * @throws IOException
   */
  public static InStream create(String name,
                                List<DiskRange> input,
                                long length,
                                CompressionCodec codec,
                                int bufferSize) throws IOException {
    if (codec == null) {
      return new UncompressedStream(name, input, length);
    } else {
      return new CompressedStream(name, input, length, codec, bufferSize);
    }
  }

  /**
   * Creates coded input stream (used for protobuf message parsing) with higher message size limit.
   *
   * @param name       the name of the stream
   * @param input      the list of ranges of bytes for the stream; from disk or cache
   * @param length     the length in bytes of the stream
   * @param codec      the compression codec
   * @param bufferSize the compression buffer size
   * @return coded input stream
   * @throws IOException
   */
  public static CodedInputStream createCodedInputStream(
      String name,
      List<DiskRange> input,//输入的原始字节内容
      long length,//字节流总长度
      CompressionCodec codec,//压缩方式
      int bufferSize) throws IOException {//压缩的缓冲区
    InStream inStream = create(name, input, length, codec, bufferSize);
    CodedInputStream codedInputStream = CodedInputStream.newInstance(inStream);
    codedInputStream.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
    return codedInputStream;
  }
}
