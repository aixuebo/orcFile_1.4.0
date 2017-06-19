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

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * Define the configuration properties that Orc understands.
 */
public enum OrcConf {
  STRIPE_SIZE("orc.stripe.size", "hive.exec.orc.default.stripe.size",
      64L * 1024 * 1024,
      "Define the default ORC stripe size, in bytes."),//stripe的大小,默认64M
  BLOCK_SIZE("orc.block.size", "hive.exec.orc.default.block.size",
      256L * 1024 * 1024,
      "Define the default file system block size for ORC files."),//文件系统数据块大小,默认256M

  ENABLE_INDEXES("orc.create.index", "orc.create.index", true,
      "Should the ORC writer create indexes as part of the file."),//是否创建索引
  ROW_INDEX_STRIDE("orc.row.index.stride",
      "hive.exec.orc.default.row.index.stride", 10000,
      "Define the default ORC index stride in number of rows. (Stride is the\n"+
          " number of rows n index entry represents.)"),//多少行进行一次索引


  BUFFER_SIZE("orc.compress.size", "hive.exec.orc.default.buffer.size",
      256 * 1024, "Define the default ORC buffer size, in bytes."),//缓冲区大小 256K
  BASE_DELTA_RATIO("orc.base.delta.ratio", "hive.exec.orc.base.delta.ratio", 8,
      "The ratio of base writer and delta writer in terms of STRIPE_SIZE and BUFFER_SIZE."),
  BLOCK_PADDING("orc.block.padding", "hive.exec.orc.default.block.padding",
      true,
      "Define whether stripes should be padded to the HDFS block boundaries."),//是否stripe去填补空字节到数据块的边界
  COMPRESS("orc.compress", "hive.exec.orc.default.compress", "ZLIB",
      "Define the default compression codec for ORC file"),//输出的内容压缩方式
  WRITE_FORMAT("orc.write.format", "hive.exec.orc.write.format", "0.12",
      "Define the version of the file to write. Possible values are 0.11 and\n"+
          " 0.12. If this parameter is not defined, ORC will use the run\n" +
          " length encoding (RLE) introduced in Hive 0.12."),
  ENCODING_STRATEGY("orc.encoding.strategy", "hive.exec.orc.encoding.strategy",
      "SPEED",
      "Define the encoding strategy to use while writing data. Changing this\n"+
          "will only affect the light weight encoding for integers. This\n" +
          "flag will not change the compression level of higher level\n" +
          "compression codec (like ZLIB)."),//编码策略
  COMPRESSION_STRATEGY("orc.compression.strategy",
      "hive.exec.orc.compression.strategy", "SPEED",
      "Define the compression strategy to use while writing data.\n" +
          "This changes the compression level of higher level compression\n" +
          "codec (like ZLIB)."),
  BLOCK_PADDING_TOLERANCE("orc.block.padding.tolerance",//填补的容忍
      "hive.exec.orc.block.padding.tolerance", 0.05,//百分比,默认5%
      "Define the tolerance for block padding as a decimal fraction of\n" +
          "stripe size (for example, the default value 0.05 is 5% of the\n" +
          "stripe size). For the defaults of 64Mb ORC stripe and 256Mb HDFS\n" +//例如一个数据块是256M,但是一个stripe是64M,
          "blocks, the default block padding tolerance of 5% will\n" +
          "reserve a maximum of 3.2Mb for padding within the 256Mb block.\n" + //意味着默认按照5%算,一个64M的数据块可以最多储备3.2M的空间。
          "In that case, if the available size within the block is more than\n"+ //在这个情况下,如果在这个数据块下可用的size如果比3.2M多,有一个很小的stripe将会插入到这空间中。
          "3.2Mb, a new smaller stripe will be inserted to fit within that\n" +
          "space. This will make sure that no stripe written will block\n" +//确保没有stripe被写入到两个数据块里面,导致的远程读取同一个数据块的问题
          " boundaries and cause remote reads within a node local task."),
  BLOOM_FILTER_FPP("orc.bloom.filter.fpp", "orc.default.bloom.fpp", 0.05,
      "Define the default false positive probability for bloom filters."),
  USE_ZEROCOPY("orc.use.zerocopy", "hive.exec.orc.zerocopy", false,
      "Use zerocopy reads with ORC. (This requires Hadoop 2.3 or later.)"),
  SKIP_CORRUPT_DATA("orc.skip.corrupt.data", "hive.exec.orc.skip.corrupt.data",
      false,
      "If ORC reader encounters corrupt data, this value will be used to\n" +
          "determine whether to skip the corrupt data or throw exception.\n" +
          "The default behavior is to throw exception."),
  TOLERATE_MISSING_SCHEMA("orc.tolerate.missing.schema",
      "hive.exec.orc.tolerate.missing.schema",
      true,
      "Writers earlier than HIVE-4243 may have inaccurate schema metadata.\n"
          + "This setting will enable best effort schema evolution rather\n"
          + "than rejecting mismatched schemas"),//true表示容忍缺失schema
  MEMORY_POOL("orc.memory.pool", "hive.exec.orc.memory.pool", 0.5,
      "Maximum fraction of heap that can be used by ORC file writers"),//占用ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()整个堆内存的百分比
  DICTIONARY_KEY_SIZE_THRESHOLD("orc.dictionary.key.threshold",
      "hive.exec.orc.dictionary.key.size.threshold",
      0.8,
      "If the number of distinct keys in a dictionary is greater than this\n" +
          "fraction of the total number of non-null rows, turn off \n" +
          "dictionary encoding.  Use 1 to always use dictionary encoding."),
  ROW_INDEX_STRIDE_DICTIONARY_CHECK("orc.dictionary.early.check",
      "hive.orc.row.index.stride.dictionary.check",
      true,
      "If enabled dictionary check will happen after first row index stride\n" +
          "(default 10000 rows) else dictionary check will happen before\n" +
          "writing first stripe. In both cases, the decision to use\n" +
          "dictionary or not will be retained thereafter."),
  BLOOM_FILTER_COLUMNS("orc.bloom.filter.columns", "orc.bloom.filter.columns",
      "", "List of columns to create bloom filters for when writing."),
  BLOOM_FILTER_WRITE_VERSION("orc.bloom.filter.write.version",
      "orc.bloom.filter.write.version", OrcFile.BloomFilterVersion.UTF8.toString(),
      "Which version of the bloom filters should we write.\n" +
          "The choices are:\n" +
          "  original - writes two versions of the bloom filters for use by\n" +
          "             both old and new readers.\n" +
          "  utf8 - writes just the new bloom filters."),
  IGNORE_NON_UTF8_BLOOM_FILTERS("orc.bloom.filter.ignore.non-utf8",
      "orc.bloom.filter.ignore.non-utf8", false,
      "Should the reader ignore the obsolete non-UTF8 bloom filters."),
  MAX_FILE_LENGTH("orc.max.file.length", "orc.max.file.length", Long.MAX_VALUE,
      "The maximum size of the file to read for finding the file tail. This\n" +
          "is primarily used for streaming ingest to read intermediate\n" +
          "footers while the file is still open"),

  MAPRED_INPUT_SCHEMA("orc.mapred.input.schema", null, null,
      "The schema that the user desires to read. The values are\n" +
      "interpreted using TypeDescription.fromString."),//输入的scheme
  MAPRED_SHUFFLE_KEY_SCHEMA("orc.mapred.map.output.key.schema", null, null,
      "The schema of the MapReduce shuffle key. The values are\n" +
          "interpreted using TypeDescription.fromString."),//shuffle中key的scheme,存储key类型的描述信息,用于确定scheme
  MAPRED_SHUFFLE_VALUE_SCHEMA("orc.mapred.map.output.value.schema", null, null,
      "The schema of the MapReduce shuffle value. The values are\n" +
          "interpreted using TypeDescription.fromString."),//shuffle中value的scheme,存储value的scheme
  MAPRED_OUTPUT_SCHEMA("orc.mapred.output.schema", null, null,
      "The schema that the user desires to write. The values are\n" +
          "interpreted using TypeDescription.fromString."),//输出的scheme


  INCLUDE_COLUMNS("orc.include.columns", "hive.io.file.readcolumn.ids", null,
      "The list of comma separated column ids that should be read with 0\n" +
          "being the first column, 1 being the next, and so on. ."),
  KRYO_SARG("orc.kryo.sarg", "orc.kryo.sarg", null,
      "The kryo and base64 encoded SearchArgument for predicate pushdown."),
  SARG_COLUMNS("orc.sarg.column.names", "org.sarg.column.names", null,
      "The list of column names for the SearchArgument."),
  FORCE_POSITIONAL_EVOLUTION("orc.force.positional.evolution",
      "orc.force.positional.evolution", false,
      "Require schema evolution to match the top level columns using position\n" +
      "rather than column names. This provides backwards compatibility with\n" +
      "Hive 2.1.")
  ;

  private final String attribute;
  private final String hiveConfName;
  private final Object defaultValue;
  private final String description;

  OrcConf(String attribute,
          String hiveConfName,
          Object defaultValue,
          String description) {
    this.attribute = attribute;
    this.hiveConfName = hiveConfName;
    this.defaultValue = defaultValue;
    this.description = description;
  }

  public String getAttribute() {
    return attribute;
  }

  public String getHiveConfName() {
    return hiveConfName;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public String getDescription() {
    return description;
  }

    /**
     * 先从tbl中获取attribute,再从conf中获取attribute,最后再从conf中获取hiveConfName
     */
  private String lookupValue(Properties tbl, Configuration conf) {
    String result = null;
    if (tbl != null) {
      result = tbl.getProperty(attribute);
    }
    if (result == null && conf != null) {
      result = conf.get(attribute);
      if (result == null && hiveConfName != null) {
        result = conf.get(hiveConfName);
      }
    }
    return result;
  }

  public long getLong(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Long.parseLong(value);
    }
    return ((Number) defaultValue).longValue();
  }

  public long getLong(Configuration conf) {
    return getLong(null, conf);
  }

  public void setLong(Configuration conf, long value) {
    conf.setLong(attribute, value);
  }

  public String getString(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    return value == null ? (String) defaultValue : value;
  }

  public String getString(Configuration conf) {
    return getString(null, conf);
  }

  public void setString(Configuration conf, String value) {
    conf.set(attribute, value);
  }

  public boolean getBoolean(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return (Boolean) defaultValue;
  }

  public boolean getBoolean(Configuration conf) {
    return getBoolean(null, conf);
  }

  public void setBoolean(Configuration conf, boolean value) {
    conf.setBoolean(attribute, value);
  }

  public double getDouble(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return ((Number) defaultValue).doubleValue();
  }

  public double getDouble(Configuration conf) {
    return getDouble(null, conf);
  }

  public void setDouble(Configuration conf, double value) {
    conf.setDouble(attribute, value);
  }
}
