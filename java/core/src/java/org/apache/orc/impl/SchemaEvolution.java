/*
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Infer and track the evolution between the schema as stored in the file and
 * the schema that has been requested by the reader.
 * 推断和演变存储在文件中的schema和用户在配置中的schema之间的不同,并且将其转换
 */
public class SchemaEvolution {
  // indexed by reader column id
  private final TypeDescription[] readerFileTypes;
  // indexed by reader column id
  private final boolean[] readerIncluded;
  // the offset to the first column id ignoring any ACID columns
  private final int readerColumnOffset;//忽略增删改查后,第一个出现的字段ID是多少
  // indexed by file column id
  private final boolean[] fileIncluded;

  private final TypeDescription fileSchema;//文件的schema
  private final TypeDescription readerSchema;//用户定义的schema
  private boolean hasConversion = false;//是否scheme有变换
  private final boolean isAcid;//true表示该文件的schema是增删改查的schema

  // indexed by reader column id
  private final boolean[] ppdSafeConversion;

  private static final Logger LOG =
    LoggerFactory.getLogger(SchemaEvolution.class);
  private static final Pattern missingMetadataPattern =
    Pattern.compile("_col\\d+");

  public static class IllegalEvolutionException extends RuntimeException {
    public IllegalEvolutionException(String msg) {
      super(msg);
    }
  }

  /**
   *
   * @param fileSchema 文件提供的schema
   * @param readerSchema 用户提供的schema
   * @param options
     */
  public SchemaEvolution(TypeDescription fileSchema,
                         TypeDescription readerSchema,
                         Reader.Options options) {
    boolean allowMissingMetadata = options.getTolerateMissingSchema();//true表示容忍缺失schema
    boolean[] includedCols = options.getInclude();
    this.readerIncluded = includedCols == null ? null :
      Arrays.copyOf(includedCols, includedCols.length);
    this.fileIncluded = new boolean[fileSchema.getMaximumId() + 1];
    this.hasConversion = false;
    this.fileSchema = fileSchema;
    isAcid = checkAcidSchema(fileSchema);//校验该schema是否是增删改查的schema
    this.readerColumnOffset = isAcid ? acidEventFieldNames.size() : 0;
    if (readerSchema != null) {
      if (isAcid) {
        this.readerSchema = createEventSchema(readerSchema);
      } else {
        this.readerSchema = readerSchema;
      }
      if (readerIncluded != null &&
          readerIncluded.length + readerColumnOffset !=
            this.readerSchema.getMaximumId() + 1) {
        throw new IllegalArgumentException("Include vector the wrong length: "
            + this.readerSchema.toJson() + " with include length "
            + readerIncluded.length);
      }
      this.readerFileTypes =
        new TypeDescription[this.readerSchema.getMaximumId() + 1];
      int positionalLevels = 0;
      if (options.getForcePositionalEvolution()) {
        positionalLevels = isAcid ? 2 : 1;
        buildConversion(fileSchema, this.readerSchema, positionalLevels);
      } else if (!hasColumnNames(isAcid? getBaseRow(fileSchema) : fileSchema)) {
        if (!this.fileSchema.equals(this.readerSchema)) {//说明schema不同
          if (!allowMissingMetadata) {//false,说明不容忍schema不同,因此要抛异常
            throw new RuntimeException("Found that schema metadata is missing"
                + " from file. This is likely caused by"
                + " a writer earlier than HIVE-4243. Will"
                + " not try to reconcile schemas");
          } else {
            LOG.warn("Column names are missing from this file. This is"
                + " caused by a writer earlier than HIVE-4243. The reader will"
                + " reconcile schemas based on index. File type: " +
                this.fileSchema + ", reader type: " + this.readerSchema);
            positionalLevels = isAcid ? 2 : 1;
          }
        }
      }
      buildConversion(fileSchema, this.readerSchema, positionalLevels);
    } else {//说明不存在用户定义的schema,因此设计用户定义的schema默认和文件的schema一样
      this.readerSchema = fileSchema;
      this.readerFileTypes =
        new TypeDescription[this.readerSchema.getMaximumId() + 1];
      if (readerIncluded != null &&
          readerIncluded.length + readerColumnOffset !=
            this.readerSchema.getMaximumId() + 1) {
        throw new IllegalArgumentException("Include vector the wrong length: "
            + this.readerSchema.toJson() + " with include length "
            + readerIncluded.length);
      }
      buildIdentityConversion(this.readerSchema);
    }
    this.ppdSafeConversion = populatePpdSafeConversion();//获取每一个字段是否允许转换
  }

  @Deprecated
  public SchemaEvolution(TypeDescription fileSchema, boolean[] readerIncluded) {
    this(fileSchema, null, readerIncluded);
  }

  @Deprecated
  public SchemaEvolution(TypeDescription fileSchema,
                         TypeDescription readerSchema,
                         boolean[] readerIncluded) {
    this(fileSchema, readerSchema,
        new Reader.Options(new Configuration())
            .include(readerIncluded));
  }

  // Return true iff all fields have names like _col[0-9]+ false表示所有的属性都是以_col[0-9]匹配的,true表示有名字不是_col[0-9]形式的
  //注意:只有Struct才有filed,因此非Struct都返回true--虽然我也不是很理解为什么,按道理应该返回false
  //返回值true表示有用户自定义的属性名字
  private boolean hasColumnNames(TypeDescription fileSchema) {
    if (fileSchema.getCategory() != TypeDescription.Category.STRUCT) {
      return true;
    }
    //代码到这里,说明一定是struct类型的
    //判断该struct类型的field是否有自己命名的name,如果有则返回true
    for (String fieldName : fileSchema.getFieldNames()) {
      if (!missingMetadataPattern.matcher(fieldName).matches()) {//有一个不匹配,则都返回true
        return true;
      }
    }
    return false;
  }

  //读取用户自定义的schema
  public TypeDescription getReaderSchema() {
    return readerSchema;
  }

  /**
   * Returns the non-ACID (aka base) reader type description.
   * 读取用户自定义的schema,但是如果是增删改查的schema,要取消增删改查增加的部分属性
   * @return the reader type ignoring the ACID rowid columns, if any
   */
  public TypeDescription getReaderBaseSchema() {
    return isAcid ? getBaseRow(readerSchema) : readerSchema;
  }

  /**
   * Does the file include ACID columns?
   * 是否是增删改查的schema
   * @return is this an ACID file?
   */
  boolean isAcid() {
    return isAcid;
  }

  /**
   * Is there Schema Evolution data type conversion?
   * 是否scheme有变换
   * @return
   */
  public boolean hasConversion() {
    return hasConversion;
  }

  public TypeDescription getFileSchema() {
    return fileSchema;
  }

  public TypeDescription getFileType(TypeDescription readerType) {
    return getFileType(readerType.getId());
  }

  /**
   * Get the file type by reader type id.
   * @param id reader column id
   * @return
   */
  public TypeDescription getFileType(int id) {
    return readerFileTypes[id];
  }

  /**
   * Get whether each column is included from the reader's point of view.
   * @return a boolean array indexed by reader column id
   */
  public boolean[] getReaderIncluded() {
    return readerIncluded;
  }

  /**
   * Get whether each column is included from the file's point of view.
   * @return a boolean array indexed by file column id
   */
  public boolean[] getFileIncluded() {
    return fileIncluded;
  }

  /**
   * Check if column is safe for ppd evaluation
   * @param colId reader column id
   * @return true if the specified column is safe for ppd evaluation else false
   * 确定该id是否允许转换,true表示可以转换
   */
  public boolean isPPDSafeConversion(final int colId) {
    if (hasConversion()) {
      return !(colId < 0 || colId >= ppdSafeConversion.length) &&
          ppdSafeConversion[colId];
    }

    // when there is no schema evolution PPD is safe
    return true;
  }

  //产生每一个scheme的id对应的是否允许转换,true表示允许转换
  private boolean[] populatePpdSafeConversion() {
    if (fileSchema == null || readerSchema == null || readerFileTypes == null) {
      return null;
    }

    boolean[] result = new boolean[readerSchema.getMaximumId() + 1];
    boolean safePpd = validatePPDConversion(fileSchema, readerSchema);
    result[readerSchema.getId()] = safePpd;
    List<TypeDescription> children = readerSchema.getChildren();
    if (children != null) {//递归查找子类的类型是否可以转换
      for (TypeDescription child : children) {
        TypeDescription fileType = getFileType(child.getId());
        safePpd = validatePPDConversion(fileType, child);
        result[child.getId()] = safePpd;
      }
    }
    return result;
  }

  //true表示可以相互转换
  private boolean validatePPDConversion(final TypeDescription fileType,
      final TypeDescription readerType) {
    if (fileType == null) {
      return false;
    }
    if (fileType.getCategory().isPrimitive()) {
      if (fileType.getCategory().equals(readerType.getCategory())) {
        // for decimals alone do equality check to not mess up with precision change
        return !(fileType.getCategory() == TypeDescription.Category.DECIMAL &&
            !fileType.equals(readerType));
      }

      // only integer and string evolutions are safe
      // byte -> short -> int -> long
      // string <-> char <-> varchar
      // NOTE: Float to double evolution is not safe as floats are stored as doubles in ORC's
      // internal index, but when doing predicate evaluation for queries like "select * from
      // orc_float where f = 74.72" the constant on the filter is converted from string -> double
      // so the precisions will be different and the comparison will fail.
      // Soon, we should convert all sargs that compare equality between floats or
      // doubles to range predicates.

      // Similarly string -> char and varchar -> char and vice versa is not possible, as ORC stores
      // char with padded spaces in its internal index.
      switch (fileType.getCategory()) {
        case BYTE:
          if (readerType.getCategory().equals(TypeDescription.Category.SHORT) ||
              readerType.getCategory().equals(TypeDescription.Category.INT) ||
              readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case SHORT:
          if (readerType.getCategory().equals(TypeDescription.Category.INT) ||
              readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case INT:
          if (readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case STRING:
          if (readerType.getCategory().equals(TypeDescription.Category.VARCHAR)) {
            return true;
          }
          break;
        case VARCHAR:
          if (readerType.getCategory().equals(TypeDescription.Category.STRING)) {
            return true;
          }
          break;
        default:
          break;
      }
    }
    return false;
  }

  /**
   * Should we read the given reader column?
   * @param readerId the id of column in the extended reader schema
   * @return true if the column should be read
   * true表示该列是可以读取的
   */
  public boolean includeReaderColumn(int readerId) {
    return readerIncluded == null ||
        readerId <= readerColumnOffset ||
        readerIncluded[readerId - readerColumnOffset];
  }

  /**
   * Build the mapping from the file type to the reader type. For pre-HIVE-4243
   * ORC files, the top level structure is matched using position within the
   * row. Otherwise, structs fields are matched by name.
   * @param fileType the type in the file
   * @param readerType the type in the reader
   * @param positionalLevels the number of structure levels that must be
   *                         mapped by position rather than field name. Pre
   *                         HIVE-4243 files have either 1 or 2 levels matched
   *                         positionally depending on whether they are ACID.
   */
  void buildConversion(TypeDescription fileType,
                       TypeDescription readerType,
                       int positionalLevels) {
    // if the column isn't included, don't map it
    if (!includeReaderColumn(readerType.getId())) {
      return;
    }
    boolean isOk = true;
    // check the easy case first
    if (fileType.getCategory() == readerType.getCategory()) {
      switch (readerType.getCategory()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DOUBLE:
        case FLOAT:
        case STRING:
        case TIMESTAMP:
        case BINARY:
        case DATE:
          // these are always a match
          break;
        case CHAR:
        case VARCHAR:
          // We do conversion when same CHAR/VARCHAR type but different
          // maxLength.不同长度的时候,我们需要转换
          if (fileType.getMaxLength() != readerType.getMaxLength()) {
            hasConversion = true;
          }
          break;
        case DECIMAL:
          // We do conversion when same DECIMAL type but different
          // precision/scale.不同精度的时候,我们需要转换
          if (fileType.getPrecision() != readerType.getPrecision() ||
              fileType.getScale() != readerType.getScale()) {
            hasConversion = true;
          }
          break;
        case UNION:
        case MAP:
        case LIST: {
          // these must be an exact match
          List<TypeDescription> fileChildren = fileType.getChildren();
          List<TypeDescription> readerChildren = readerType.getChildren();
          if (fileChildren.size() == readerChildren.size()) {
            for(int i=0; i < fileChildren.size(); ++i) {//比较里面的每一个子对象
              buildConversion(fileChildren.get(i),
                              readerChildren.get(i), 0);
            }
          } else {//size都不同,说明isOk=false
            isOk = false;
          }
          break;
        }
        case STRUCT: {
          List<TypeDescription> readerChildren = readerType.getChildren();
          List<TypeDescription> fileChildren = fileType.getChildren();
          if (fileChildren.size() != readerChildren.size()) {
            hasConversion = true;
          }

          if (positionalLevels == 0) {
            List<String> readerFieldNames = readerType.getFieldNames();
            List<String> fileFieldNames = fileType.getFieldNames();
            Map<String, TypeDescription> fileTypesIdx = new HashMap<>();//文件中schema的每一个属性---下标映射关系
            for (int i = 0; i < fileFieldNames.size(); i++) {//循环每一个文件中的属性
              fileTypesIdx.put(fileFieldNames.get(i), fileChildren.get(i));
            }

            for (int i = 0; i < readerFieldNames.size(); i++) {//循环每一个自定义的schema的属性
              String readerFieldName = readerFieldNames.get(i);
              TypeDescription readerField = readerChildren.get(i);//自定义的该属性对应的类型

              TypeDescription fileField = fileTypesIdx.get(readerFieldName);//文件中该属性对应的类型
              if (fileField == null) {//说明文件中不包含该属性
                continue;
              }

              buildConversion(fileField, readerField, 0);//转换
            }
          } else {
            int jointSize = Math.min(fileChildren.size(),
                                     readerChildren.size());
            for (int i = 0; i < jointSize; ++i) {
              buildConversion(fileChildren.get(i), readerChildren.get(i),
                  positionalLevels - 1);
            }
          }
          break;
        }
        default:
          throw new IllegalArgumentException("Unknown type " + readerType);
      }
    } else {
      /*
       * Check for the few cases where will not convert....
       */

      isOk = ConvertTreeReaderFactory.canConvert(fileType, readerType);//说明可以转换
      hasConversion = true;
    }
    if (isOk) {//说明可以转换
      readerFileTypes[readerType.getId()] = fileType;
      fileIncluded[fileType.getId()] = true;
    } else {//isOk=false,说明不能转换
      throw new IllegalEvolutionException(
          String.format("ORC does not support type conversion from file" +
                        " type %s (%d) to reader type %s (%d)",
                        fileType.toString(), fileType.getId(),
                        readerType.toString(), readerType.getId()));
    }
  }

  void buildIdentityConversion(TypeDescription readerType) {
    int id = readerType.getId();
    if (!includeReaderColumn(id)) {//说明该列不能读取
      return;
    }
    if (readerFileTypes[id] != null) {//此时必须是null
      throw new RuntimeException("reader to file type entry already assigned");
    }
    readerFileTypes[id] = readerType;//设置该列的类型
    fileIncluded[id] = true;
    List<TypeDescription> children = readerType.getChildren();
    if (children != null) {
      for (TypeDescription child : children) {
        buildIdentityConversion(child);
      }
    }
  }

    //true表示该对象是事件对象
  private static boolean checkAcidSchema(TypeDescription type) {
    if (type.getCategory().equals(TypeDescription.Category.STRUCT)) {
      List<String> rootFields = type.getFieldNames();
      if (acidEventFieldNames.equals(rootFields)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param typeDescr
   * @return ORC types for the ACID event based on the row's type description
   * 创建一个事件scheme,
   */
  public static TypeDescription createEventSchema(TypeDescription typeDescr) {
    TypeDescription result = TypeDescription.createStruct()
        .addField("operation", TypeDescription.createInt())//int类型
        .addField("originalTransaction", TypeDescription.createLong())//long类型
        .addField("bucket", TypeDescription.createInt())//int类型
        .addField("rowId", TypeDescription.createLong())//long类型
        .addField("currentTransaction", TypeDescription.createLong())//long类型
        .addField("row", typeDescr.clone());//事件也是包含原始的内容
    return result;
  }

  /**
   * Get the underlying base row from an ACID event struct.
   * @param typeDescription the ACID event schema.
   * @return the subtype for the real row
   * 获取事件对象中原始的类型
   */
  static TypeDescription getBaseRow(TypeDescription typeDescription) {
    final int ACID_ROW_OFFSET = 5;//因为原始的数据类型是第5个
    return typeDescription.getChildren().get(ACID_ROW_OFFSET);
  }

  //事件增加的列字段集合
  private static final List<String> acidEventFieldNames=
    new ArrayList<String>();

  static {
    acidEventFieldNames.add("operation");
    acidEventFieldNames.add("originalTransaction");
    acidEventFieldNames.add("bucket");
    acidEventFieldNames.add("rowId");
    acidEventFieldNames.add("currentTransaction");
    acidEventFieldNames.add("row");
  }
}
