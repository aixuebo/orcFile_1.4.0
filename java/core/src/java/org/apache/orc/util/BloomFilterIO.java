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

package org.apache.orc.util;

import com.google.protobuf.ByteString;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

//如何对BloomFilter对象进行序列化与反序列化
public class BloomFilterIO  {

  public enum Encoding {
    ORIGINAL(0),
    UTF8_UTC(1),
    FUTURE(Integer.MAX_VALUE);

    public static final Encoding CURRENT = UTF8_UTC;

    private final int id;

    Encoding(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    public static Encoding from(OrcProto.ColumnEncoding encoding) {
      if (!encoding.hasBloomEncoding()) {//说明没有设置该参数
        return ORIGINAL;//则设置默认值
      }
      switch (encoding.getBloomEncoding()) {
        case 0:
          return ORIGINAL;
        case 1:
          return UTF8_UTC;
        default:
          return FUTURE;
      }
    }
  }

  private BloomFilterIO() {
    // never called
  }

  /**
   * Deserialize a bloom filter from the ORC file.
   * 对BloomFilter对象反序列化
   */
  public static BloomFilter deserialize(OrcProto.Stream.Kind kind,
                                        OrcProto.ColumnEncoding encoding,
                                        OrcFile.WriterVersion fileVersion,
                                        TypeDescription.Category type,
                                        OrcProto.BloomFilter bloomFilter) {
    if (bloomFilter == null) {
      return null;
    }
    int numFuncs = bloomFilter.getNumHashFunctions();
    switch (kind) {
      case BLOOM_FILTER: {
        long values[] = new long[bloomFilter.getBitsetCount()];//反序列化BloomFilter的内容
        for (int i = 0; i < values.length; ++i) {
          values[i] = bloomFilter.getBitset(i);
        }
        // After HIVE-12055 the bloom filters for strings correctly use
        // UTF8.
        if (fileVersion.includes(OrcFile.WriterVersion.HIVE_12055) &&
            (type == TypeDescription.Category.STRING ||
             type == TypeDescription.Category.CHAR ||
             type == TypeDescription.Category.VARCHAR)) {//字符串使用UTF-8
          return new BloomFilterUtf8(values, numFuncs);
        }
        return new BloomFilter(values, numFuncs);
      }
      case BLOOM_FILTER_UTF8: {
        // make sure we don't use unknown encodings or original timestamp encodings
        Encoding version = Encoding.from(encoding);
        if (version == Encoding.FUTURE ||
            (type == TypeDescription.Category.TIMESTAMP &&
                version == Encoding.ORIGINAL)) {
          return null;
        }
        ByteString bits = bloomFilter.getUtf8Bitset();
        long[] values = new long[bits.size() / 8];
        bits.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN)
            .asLongBuffer().get(values);
        return new BloomFilterUtf8(values, numFuncs);
      }
      default:
        throw new IllegalArgumentException("Unknown bloom filter kind " + kind);
    }
  }

  /**
   * Serialize the BloomFilter to the ORC file.
   * @param builder the builder to write to
   * @param bloomFilter the bloom filter to serialize
   * 序列化一个BloomFilter对象
   */
  public static void serialize(OrcProto.BloomFilter.Builder builder,
                               BloomFilter bloomFilter) {
    builder.clear();
    builder.setNumHashFunctions(bloomFilter.getNumHashFunctions());//设置多少个hash函数
    long[] bitset = bloomFilter.getBitSet();//获取存储BloomFilter的long值
    if (bloomFilter instanceof BloomFilterUtf8) {
      ByteBuffer buffer = ByteBuffer.allocate(bitset.length * 8);//分配这些long所有的空间
      buffer.order(ByteOrder.LITTLE_ENDIAN);
      buffer.asLongBuffer().put(bitset);
      builder.setUtf8Bitset(ByteString.copyFrom(buffer));
    } else {
      for(int i=0; i < bitset.length; ++i) {//设置每一个long的值
        builder.addBitset(bitset[i]);
      }
    }
  }
}
