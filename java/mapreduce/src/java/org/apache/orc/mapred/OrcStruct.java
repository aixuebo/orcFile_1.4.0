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
package org.apache.orc.mapred;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 对一个schema类型的数据进行存储具体的值.按照属性不同,划分不同的值
 */
public final class OrcStruct implements WritableComparable<OrcStruct> {

  private WritableComparable[] fields;//该类型下所有的子类型,存储每一个类型对应的值
  private final TypeDescription schema;//类型

  public OrcStruct(TypeDescription schema) {
    this.schema = schema;
    fields = new WritableComparable[schema.getChildren().size()];
  }

  public WritableComparable getFieldValue(int fieldIndex) {
    return fields[fieldIndex];
  }//获取某一个列的类型

  public void setFieldValue(int fieldIndex, WritableComparable value) {
    fields[fieldIndex] = value;
  }

  public int getNumFields() {
    return fields.length;
  }//列数量

  @Override
  public void write(DataOutput output) throws IOException {
    for(WritableComparable field: fields) {//将每一列的内容输出到output中
      output.writeBoolean(field != null);//设置该列是否是null
      if (field != null) {
        field.write(output);
      }
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    for(int f=0; f < fields.length; ++f) {
      if (input.readBoolean()) {
        if (fields[f] == null) {
          fields[f] = createValue(schema.getChildren().get(f));//为每一个类型创建一个WritableComparable对象
        }
        fields[f].readFields(input);
      } else {
        fields[f] = null;
      }
    }
  }

  /**
   * Get the schema for this object.
   * @return the schema object
   */
  public TypeDescription getSchema() {
    return schema;
  }

  /**
   * Set all of the fields in the struct
   * @param values the list of values for each of the fields.
   */
  public void setAllFields(WritableComparable... values) {
    if (fields.length != values.length) {
      throw new IllegalArgumentException("Wrong number (" + values.length +
          ") of fields for " + schema);
    }
    for (int col = 0; col < fields.length && col < values.length; ++col) {
      fields[col] = values[col];
    }
  }

    //设置某一个列的值
  public void setFieldValue(String fieldName, WritableComparable value) {
    int fieldIdx = schema.getFieldNames().indexOf(fieldName);//找到下标
    if (fieldIdx == -1) {
      throw new IllegalArgumentException("Field " + fieldName +
          " not found in " + schema);
    }
    fields[fieldIdx] = value;//设置该下标对应的值
  }

    //获取该列对应的值
  public WritableComparable getFieldValue(String fieldName) {
    int fieldIdx = schema.getFieldNames().indexOf(fieldName);
    if (fieldIdx == -1) {
      throw new IllegalArgumentException("Field " + fieldName +
          " not found in " + schema);
    }
    return fields[fieldIdx];
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != OrcStruct.class) {
      return false;
    } else {
      OrcStruct oth = (OrcStruct) other;
      if (fields.length != oth.fields.length) {
        return false;
      }
      for(int i=0; i < fields.length; ++i) {
        if (fields[i] == null) {
          if (oth.fields[i] != null) {
            return false;
          }
        } else {
          if (!fields[i].equals(oth.fields[i])) {
            return false;
          }
        }
      }
      return true;
    }
  }

  @Override
  public int hashCode() {
    int result = fields.length;
    for(Object field: fields) {
      if (field != null) {
        result ^= field.hashCode();
      }
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{");
    for(int i=0; i < fields.length; ++i) {
      if (i != 0) {
        buffer.append(", ");
      }
      buffer.append(fields[i]);
    }
    buffer.append("}");
    return buffer.toString();
  }

  /* Routines for stubbing into Writables */
  //根据不同类型,转换成不同的序列化对象---该对象用于存储具体的值
  public static WritableComparable createValue(TypeDescription type) {
    switch (type.getCategory()) {
      case BOOLEAN: return new BooleanWritable();
      case BYTE: return new ByteWritable();
      case SHORT: return new ShortWritable();
      case INT: return new IntWritable();
      case LONG: return new LongWritable();
      case FLOAT: return new FloatWritable();
      case DOUBLE: return new DoubleWritable();
      case BINARY: return new BytesWritable();
      case CHAR:
      case VARCHAR:
      case STRING:
        return new Text();
      case DATE:
        return new DateWritable();//以上都使用hadoop自带的序列化工具
      case TIMESTAMP:
        return new OrcTimestamp();
      case DECIMAL:
        return new HiveDecimalWritable();//使用hive的序列化工具
      case STRUCT: {
        OrcStruct result = new OrcStruct(type);
        int c = 0;
        for(TypeDescription child: type.getChildren()) {
          result.setFieldValue(c++, createValue(child));
        }
        return result;
      }
      case UNION: return new OrcUnion(type);
      case LIST: return new OrcList(type);
      case MAP: return new OrcMap(type);
      default:
        throw new IllegalArgumentException("Unknown type " + type);
    }
  }

  @Override
  public int compareTo(OrcStruct other) {
    if (other == null) {
      return -1;
    }
    int result = schema.compareTo(other.schema);
    if (result != 0) {
      return result;
    }
    for(int c = 0; c < fields.length && c < other.fields.length; ++c) {
      if (fields[c] == null) {
        if (other.fields[c] != null) {
          return 1;
        }
      } else if (other.fields[c] == null) {
        return -1;
      } else {
        int val = fields[c].compareTo(other.fields[c]);
        if (val != 0) {
          return val;
        }
      }
    }
    return fields.length - other.fields.length;
  }
}
