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

import org.apache.orc.impl.ReaderImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OrcUtils {

  /**
   * Returns selected columns as a boolean array with true value set for specified column names.
   * The result will contain number of elements equal to flattened number of columns.
   * For example:
   * selectedColumns - a,b,c
   * allColumns - a,b,c,d
   * If column c is a complex type, say list&lt;string&gt; and other types are
   * primitives then result will
   * be [false, true, true, true, true, true, false]
   * Index 0 is the root element of the struct which is set to false by default, index 1,2
   * corresponds to columns a and b. Index 3,4 correspond to column c which is list&lt;string&gt; and
   * index 5 correspond to column d. After flattening list&lt;string&gt; gets 2 columns.
   *
   * @param selectedColumns - comma separated list of selected column names 选择的列集合,用逗号拆分
   * @param schema       - object schema
   * @return - boolean array with true value set for the specified column names,返回属性包含的哪些列,这些列对应的位置是true
   */
  public static boolean[] includeColumns(String selectedColumns,
                                         TypeDescription schema) {
    int numFlattenedCols = schema.getMaximumId();//一共有多少个属性
    boolean[] results = new boolean[numFlattenedCols + 1];//因为下标从1开始,因此结果是有+1
    if ("*".equals(selectedColumns)) {
      Arrays.fill(results, true);//设置所有列都是true
      return results;
    }
    if (selectedColumns != null &&
        schema.getCategory() == TypeDescription.Category.STRUCT) {//必须是struct类型
      List<String> fieldNames = schema.getFieldNames();//所有属性集合
      List<TypeDescription> fields = schema.getChildren();//所有属性对应的类型
      for (String column: selectedColumns.split((","))) {
        TypeDescription col = findColumn(column, fieldNames, fields);//找到选择的列对象类型
        if (col != null) {
          for(int i=col.getId(); i <= col.getMaximumId(); ++i) {
            results[i] = true;
          }
        }
      }
    }
    return results;
  }

    /**
     * 获取struct对象中columnName对应的属性对象类型
     * @param columnName 要查找的属性名字
     * @param fieldNames struct对象对应的属性名字集合
     * @param fields struct对象每一个属性对应的对象集合
     * @return
     */
  private static TypeDescription findColumn(String columnName,
                                            List<String> fieldNames,
                                            List<TypeDescription> fields) {
    int i = 0;
    for(String fieldName: fieldNames) {
      if (fieldName.equalsIgnoreCase(columnName)) {
        return fields.get(i);
      } else {
        i += 1;
      }
    }
    return null;
  }

  //将typeDescr对象scheme整理成Type对象集合
  public static List<OrcProto.Type> getOrcTypes(TypeDescription typeDescr) {
    List<OrcProto.Type> result = new ArrayList<>();
    appendOrcTypes(result, typeDescr);
    return result;
  }

  //将typeDescr对象scheme整理成Type对象集合
  private static void appendOrcTypes(List<OrcProto.Type> result, TypeDescription typeDescr) {
    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
    List<TypeDescription> children = typeDescr.getChildren();
    switch (typeDescr.getCategory()) {
    case BOOLEAN:
      type.setKind(OrcProto.Type.Kind.BOOLEAN);
      break;
    case BYTE:
      type.setKind(OrcProto.Type.Kind.BYTE);
      break;
    case SHORT:
      type.setKind(OrcProto.Type.Kind.SHORT);
      break;
    case INT:
      type.setKind(OrcProto.Type.Kind.INT);
      break;
    case LONG:
      type.setKind(OrcProto.Type.Kind.LONG);
      break;
    case FLOAT:
      type.setKind(OrcProto.Type.Kind.FLOAT);
      break;
    case DOUBLE:
      type.setKind(OrcProto.Type.Kind.DOUBLE);
      break;
    case STRING:
      type.setKind(OrcProto.Type.Kind.STRING);
      break;
    case CHAR:
      type.setKind(OrcProto.Type.Kind.CHAR);
      type.setMaximumLength(typeDescr.getMaxLength());
      break;
    case VARCHAR:
      type.setKind(OrcProto.Type.Kind.VARCHAR);
      type.setMaximumLength(typeDescr.getMaxLength());
      break;
    case BINARY:
      type.setKind(OrcProto.Type.Kind.BINARY);
      break;
    case TIMESTAMP:
      type.setKind(OrcProto.Type.Kind.TIMESTAMP);
      break;
    case DATE:
      type.setKind(OrcProto.Type.Kind.DATE);
      break;
    case DECIMAL:
      type.setKind(OrcProto.Type.Kind.DECIMAL);
      type.setPrecision(typeDescr.getPrecision());
      type.setScale(typeDescr.getScale());
      break;
    case LIST:
      type.setKind(OrcProto.Type.Kind.LIST);
      type.addSubtypes(children.get(0).getId());
      break;
    case MAP:
      type.setKind(OrcProto.Type.Kind.MAP);
      for(TypeDescription t: children) {
        type.addSubtypes(t.getId());
      }
      break;
    case STRUCT:
      type.setKind(OrcProto.Type.Kind.STRUCT);
      for(TypeDescription t: children) {
        type.addSubtypes(t.getId());
      }
      for(String field: typeDescr.getFieldNames()) {
        type.addFieldNames(field);
      }
      break;
    case UNION:
      type.setKind(OrcProto.Type.Kind.UNION);
      for(TypeDescription t: children) {
        type.addSubtypes(t.getId());
      }
      break;
    default:
      throw new IllegalArgumentException("Unknown category: " +
          typeDescr.getCategory());
    }
    result.add(type.build());
    if (children != null) {
      for(TypeDescription child: children) {
        appendOrcTypes(result, child);
      }
    }
  }

  /**
   * NOTE: This method ignores the subtype numbers in the TypeDescription rebuilds the subtype
   * numbers based on the length of the result list being appended.
   *
   * @param result
   * @param typeDescr
   */
  public static void appendOrcTypesRebuildSubtypes(List<OrcProto.Type> result,
      TypeDescription typeDescr) {

    int subtype = result.size();
    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
    boolean needsAdd = true;
    List<TypeDescription> children = typeDescr.getChildren();
    switch (typeDescr.getCategory()) {
    case BOOLEAN:
      type.setKind(OrcProto.Type.Kind.BOOLEAN);
      break;
    case BYTE:
      type.setKind(OrcProto.Type.Kind.BYTE);
      break;
    case SHORT:
      type.setKind(OrcProto.Type.Kind.SHORT);
      break;
    case INT:
      type.setKind(OrcProto.Type.Kind.INT);
      break;
    case LONG:
      type.setKind(OrcProto.Type.Kind.LONG);
      break;
    case FLOAT:
      type.setKind(OrcProto.Type.Kind.FLOAT);
      break;
    case DOUBLE:
      type.setKind(OrcProto.Type.Kind.DOUBLE);
      break;
    case STRING:
      type.setKind(OrcProto.Type.Kind.STRING);
      break;
    case CHAR:
      type.setKind(OrcProto.Type.Kind.CHAR);
      type.setMaximumLength(typeDescr.getMaxLength());
      break;
    case VARCHAR:
      type.setKind(OrcProto.Type.Kind.VARCHAR);
      type.setMaximumLength(typeDescr.getMaxLength());
      break;
    case BINARY:
      type.setKind(OrcProto.Type.Kind.BINARY);
      break;
    case TIMESTAMP:
      type.setKind(OrcProto.Type.Kind.TIMESTAMP);
      break;
    case DATE:
      type.setKind(OrcProto.Type.Kind.DATE);
      break;
    case DECIMAL:
      type.setKind(OrcProto.Type.Kind.DECIMAL);
      type.setPrecision(typeDescr.getPrecision());
      type.setScale(typeDescr.getScale());
      break;
    case LIST:
      type.setKind(OrcProto.Type.Kind.LIST);
      type.addSubtypes(++subtype);
      result.add(type.build());
      needsAdd = false;
      appendOrcTypesRebuildSubtypes(result, children.get(0));
      break;
    case MAP:
      {
        // Make room for MAP type.
        result.add(null);
  
        // Add MAP type pair in order to determine their subtype values.
        appendOrcTypesRebuildSubtypes(result, children.get(0));
        int subtype2 = result.size();
        appendOrcTypesRebuildSubtypes(result, children.get(1));
        type.setKind(OrcProto.Type.Kind.MAP);
        type.addSubtypes(subtype + 1);
        type.addSubtypes(subtype2);
        result.set(subtype, type.build());
        needsAdd = false;
      }
      break;
    case STRUCT:
      {
        List<String> fieldNames = typeDescr.getFieldNames();

        // Make room for STRUCT type.
        result.add(null);

        List<Integer> fieldSubtypes = new ArrayList<Integer>(fieldNames.size());
        for(TypeDescription child: children) {
          int fieldSubtype = result.size();
          fieldSubtypes.add(fieldSubtype);
          appendOrcTypesRebuildSubtypes(result, child);
        }

        type.setKind(OrcProto.Type.Kind.STRUCT);

        for (int i = 0 ; i < fieldNames.size(); i++) {
          type.addSubtypes(fieldSubtypes.get(i));
          type.addFieldNames(fieldNames.get(i));
        }
        result.set(subtype, type.build());
        needsAdd = false;
      }
      break;
    case UNION:
      {
        // Make room for UNION type.
        result.add(null);

        List<Integer> unionSubtypes = new ArrayList<Integer>(children.size());
        for(TypeDescription child: children) {
          int unionSubtype = result.size();
          unionSubtypes.add(unionSubtype);
          appendOrcTypesRebuildSubtypes(result, child);
        }

        type.setKind(OrcProto.Type.Kind.UNION);
        for (int i = 0 ; i < children.size(); i++) {
          type.addSubtypes(unionSubtypes.get(i));
        }
        result.set(subtype, type.build());
        needsAdd = false;
      }
      break;
    default:
      throw new IllegalArgumentException("Unknown category: " + typeDescr.getCategory());
    }
    if (needsAdd) {
      result.add(type.build());
    }
  }

  /**
   * NOTE: This method ignores the subtype numbers in the OrcProto.Type rebuilds the subtype
   * numbers based on the length of the result list being appended.
   *
   * @param result
   * @param types
   * @param columnId
   */
  public static int appendOrcTypesRebuildSubtypes(List<OrcProto.Type> result,
      List<OrcProto.Type> types, int columnId) {

    OrcProto.Type oldType = types.get(columnId++);

    int subtype = result.size();
    OrcProto.Type.Builder builder = OrcProto.Type.newBuilder();
    boolean needsAdd = true;
    switch (oldType.getKind()) {
    case BOOLEAN:
      builder.setKind(OrcProto.Type.Kind.BOOLEAN);
      break;
    case BYTE:
      builder.setKind(OrcProto.Type.Kind.BYTE);
      break;
    case SHORT:
      builder.setKind(OrcProto.Type.Kind.SHORT);
      break;
    case INT:
      builder.setKind(OrcProto.Type.Kind.INT);
      break;
    case LONG:
      builder.setKind(OrcProto.Type.Kind.LONG);
      break;
    case FLOAT:
      builder.setKind(OrcProto.Type.Kind.FLOAT);
      break;
    case DOUBLE:
      builder.setKind(OrcProto.Type.Kind.DOUBLE);
      break;
    case STRING:
      builder.setKind(OrcProto.Type.Kind.STRING);
      break;
    case CHAR:
      builder.setKind(OrcProto.Type.Kind.CHAR);
      builder.setMaximumLength(oldType.getMaximumLength());
      break;
    case VARCHAR:
      builder.setKind(OrcProto.Type.Kind.VARCHAR);
      builder.setMaximumLength(oldType.getMaximumLength());
      break;
    case BINARY:
      builder.setKind(OrcProto.Type.Kind.BINARY);
      break;
    case TIMESTAMP:
      builder.setKind(OrcProto.Type.Kind.TIMESTAMP);
      break;
    case DATE:
      builder.setKind(OrcProto.Type.Kind.DATE);
      break;
    case DECIMAL:
      builder.setKind(OrcProto.Type.Kind.DECIMAL);
      builder.setPrecision(oldType.getPrecision());
      builder.setScale(oldType.getScale());
      break;
    case LIST:
      builder.setKind(OrcProto.Type.Kind.LIST);
      builder.addSubtypes(++subtype);
      result.add(builder.build());
      needsAdd = false;
      columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
      break;
    case MAP:
      {
        // Make room for MAP type.
        result.add(null);
  
        // Add MAP type pair in order to determine their subtype values.
        columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
        int subtype2 = result.size();
        columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
        builder.setKind(OrcProto.Type.Kind.MAP);
        builder.addSubtypes(subtype + 1);
        builder.addSubtypes(subtype2);
        result.set(subtype, builder.build());
        needsAdd = false;
      }
      break;
    case STRUCT:
      {
        List<String> fieldNames = oldType.getFieldNamesList();

        // Make room for STRUCT type.
        result.add(null);

        List<Integer> fieldSubtypes = new ArrayList<Integer>(fieldNames.size());
        for(int i = 0 ; i < fieldNames.size(); i++) {
          int fieldSubtype = result.size();
          fieldSubtypes.add(fieldSubtype);
          columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
        }

        builder.setKind(OrcProto.Type.Kind.STRUCT);

        for (int i = 0 ; i < fieldNames.size(); i++) {
          builder.addSubtypes(fieldSubtypes.get(i));
          builder.addFieldNames(fieldNames.get(i));
        }
        result.set(subtype, builder.build());
        needsAdd = false;
      }
      break;
    case UNION:
      {
        int subtypeCount = oldType.getSubtypesCount();

        // Make room for UNION type.
        result.add(null);

        List<Integer> unionSubtypes = new ArrayList<Integer>(subtypeCount);
        for(int i = 0 ; i < subtypeCount; i++) {
          int unionSubtype = result.size();
          unionSubtypes.add(unionSubtype);
          columnId = appendOrcTypesRebuildSubtypes(result, types, columnId);
        }

        builder.setKind(OrcProto.Type.Kind.UNION);
        for (int i = 0 ; i < subtypeCount; i++) {
          builder.addSubtypes(unionSubtypes.get(i));
        }
        result.set(subtype, builder.build());
        needsAdd = false;
      }
      break;
    default:
      throw new IllegalArgumentException("Unknown category: " + oldType.getKind());
    }
    if (needsAdd) {
      result.add(builder.build());
    }
    return columnId;
  }

  /**
   * 转换成类型对象
   * Translate the given rootColumn from the list of types to a TypeDescription.
   * @param types all of the types 所有的类型
   * @param rootColumn translate this type 要转换的类型序号
   * @return a new TypeDescription that matches the given rootColumn
   */
  public static
        TypeDescription convertTypeFromProtobuf(List<OrcProto.Type> types,
                                                int rootColumn) {
    OrcProto.Type type = types.get(rootColumn);//找到要转换的具体类型对象
    switch (type.getKind()) {//根据不同的类型,转换成类型对象
      case BOOLEAN:
        return TypeDescription.createBoolean();
      case BYTE:
        return TypeDescription.createByte();
      case SHORT:
        return TypeDescription.createShort();
      case INT:
        return TypeDescription.createInt();
      case LONG:
        return TypeDescription.createLong();
      case FLOAT:
        return TypeDescription.createFloat();
      case DOUBLE:
        return TypeDescription.createDouble();
      case STRING:
        return TypeDescription.createString();
      case CHAR:
      case VARCHAR: {
        TypeDescription result = type.getKind() == OrcProto.Type.Kind.CHAR ?
            TypeDescription.createChar() : TypeDescription.createVarchar();
        if (type.hasMaximumLength()) {
          result.withMaxLength(type.getMaximumLength());
        }
        return result;
      }
      case BINARY:
        return TypeDescription.createBinary();
      case TIMESTAMP:
        return TypeDescription.createTimestamp();
      case DATE:
        return TypeDescription.createDate();
      case DECIMAL: {
        TypeDescription result = TypeDescription.createDecimal();
        if (type.hasScale()) {
          result.withScale(type.getScale());
        }
        if (type.hasPrecision()) {
          result.withPrecision(type.getPrecision());
        }
        return result;
      }
      case LIST:
        return TypeDescription.createList(
            convertTypeFromProtobuf(types, type.getSubtypes(0)));//type.getSubtypes(0) 表示获取第一个子对象对应的ID
      case MAP:
        return TypeDescription.createMap(
            convertTypeFromProtobuf(types, type.getSubtypes(0)),
            convertTypeFromProtobuf(types, type.getSubtypes(1)));
      case STRUCT: {
        TypeDescription result = TypeDescription.createStruct();
        for(int f=0; f < type.getSubtypesCount(); ++f) {
          result.addField(type.getFieldNames(f),
              convertTypeFromProtobuf(types, type.getSubtypes(f)));
        }
        return result;
      }
      case UNION: {
        TypeDescription result = TypeDescription.createUnion();
        for(int f=0; f < type.getSubtypesCount(); ++f) {
          result.addUnionChild(
              convertTypeFromProtobuf(types, type.getSubtypes(f)));
        }
        return result;
      }
    }
    throw new IllegalArgumentException("Unknown ORC type " + type.getKind());
  }

  public static List<StripeInformation> convertProtoStripesToStripes(
      List<OrcProto.StripeInformation> stripes) {
    List<StripeInformation> result = new ArrayList<StripeInformation>(stripes.size());
    for (OrcProto.StripeInformation info : stripes) {
      result.add(new ReaderImpl.StripeInformationImpl(info));
    }
    return result;
  }
}
