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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This is the description of the types in an ORC file.
 */
public class TypeDescription
    implements Comparable<TypeDescription>, Serializable, Cloneable {
  private static final int MAX_PRECISION = 38;
  private static final int MAX_SCALE = 38;
  private static final int DEFAULT_PRECISION = 38;
  private static final int DEFAULT_SCALE = 10;
  private static final int DEFAULT_LENGTH = 256;//最大长度固定值
  private static final Pattern UNQUOTED_NAMES = Pattern.compile("^\\w+$");//匹配任意单词---用于查找STRUCT中的name

  @Override
  public int compareTo(TypeDescription other) {
    if (this == other) {
      return 0;
    } else if (other == null) {
      return -1;
    } else {
      int result = category.compareTo(other.category);
      if (result == 0) {//说明分类相同
        switch (category) {
          case CHAR:
          case VARCHAR:
            return maxLength - other.maxLength;
          case DECIMAL:
            if (precision != other.precision) {
              return precision - other.precision;
            }
            return scale - other.scale;
          case UNION:
          case LIST:
          case MAP:
            if (children.size() != other.children.size()) {
              return children.size() - other.children.size();
            }
            for(int c=0; result == 0 && c < children.size(); ++c) {
              result = children.get(c).compareTo(other.children.get(c));
            }
            break;
          case STRUCT:
            if (children.size() != other.children.size()) {
              return children.size() - other.children.size();
            }
            for(int c=0; result == 0 && c < children.size(); ++c) {
              result = fieldNames.get(c).compareTo(other.fieldNames.get(c));
              if (result == 0) {
                result = children.get(c).compareTo(other.children.get(c));
              }
            }
            break;
          default:
            // PASS
        }
      }
      return result;
    }
  }

    //属性类型名字以及是否是原生类型
  public enum Category {
    BOOLEAN("boolean", true),
    BYTE("tinyint", true),
    SHORT("smallint", true),
    INT("int", true),
    LONG("bigint", true),
    FLOAT("float", true),
    DOUBLE("double", true),
    STRING("string", true),
    DATE("date", true),
    TIMESTAMP("timestamp", true),
    BINARY("binary", true),
    DECIMAL("decimal", true),
    VARCHAR("varchar", true),
    CHAR("char", true),
    LIST("array", false),//说明非原生类型,包含一种类型
    MAP("map", false),//定义两种类型,分别key和value,不需要name,因为就两个类型,顺序决定好就可以了
    STRUCT("struct", false),//相当于对象,定义若干种类型,每一个name对应一个类型
    UNION("uniontype", false);//存储多个类型元素,相当于List<Object>

    Category(String name, boolean isPrimitive) {
      this.name = name;
      this.isPrimitive = isPrimitive;
    }

    final boolean isPrimitive;
    final String name;

    public boolean isPrimitive() {
      return isPrimitive;
    }

    public String getName() {
      return name;
    }
  }

  public static TypeDescription createBoolean() {
    return new TypeDescription(Category.BOOLEAN);
  }

  public static TypeDescription createByte() {
    return new TypeDescription(Category.BYTE);
  }

  public static TypeDescription createShort() {
    return new TypeDescription(Category.SHORT);
  }

  public static TypeDescription createInt() {
    return new TypeDescription(Category.INT);
  }

  public static TypeDescription createLong() {
    return new TypeDescription(Category.LONG);
  }

  public static TypeDescription createFloat() {
    return new TypeDescription(Category.FLOAT);
  }

  public static TypeDescription createDouble() {
    return new TypeDescription(Category.DOUBLE);
  }

  public static TypeDescription createString() {
    return new TypeDescription(Category.STRING);
  }

  public static TypeDescription createDate() {
    return new TypeDescription(Category.DATE);
  }

  public static TypeDescription createTimestamp() {
    return new TypeDescription(Category.TIMESTAMP);
  }

  public static TypeDescription createBinary() {
    return new TypeDescription(Category.BINARY);
  }

  public static TypeDescription createDecimal() {
    return new TypeDescription(Category.DECIMAL);
  }

  static class StringPosition {
    final String value;//字符串的全部内容,等待去解析
    int position;//当前解析到第几个字节了
    final int length;//一共value多少个字节

    StringPosition(String value) {
      this.value = value;
      position = 0;
      length = value.length();
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append('\'');
      buffer.append(value.substring(0, position));
      buffer.append('^');
      buffer.append(value.substring(position));
      buffer.append('\'');
      return buffer.toString();
    }
  }

    //解析类型
  static Category parseCategory(StringPosition source) {
    int start = source.position;//设置开始位置
    while (source.position < source.length) {//不断的解析
      char ch = source.value.charAt(source.position);//每次解析一个字节
      if (!Character.isLetter(ch)) {
        break;
      }
      source.position += 1;//移动指针
    }
    if (source.position != start) {//说明移动了,因此找到类型
      String word = source.value.substring(start, source.position).toLowerCase();//截取类型,获取大写形式
      for (Category cat : Category.values()) {//找到对应的类型
        if (cat.getName().equals(word)) {
          return cat;
        }
      }
    }
    throw new IllegalArgumentException("Can't parse category at " + source);
  }

    //解析一个整数出来
  static int parseInt(StringPosition source) {
    int start = source.position;
    int result = 0;
    while (source.position < source.length) {//不断循环
      char ch = source.value.charAt(source.position);//获取一个字节
      if (!Character.isDigit(ch)) {//必须是数字
        break;
      }
      result = result * 10 + (ch - '0');//每次*10
      source.position += 1;
    }
    if (source.position == start) {
      throw new IllegalArgumentException("Missing integer at " + source);
    }
    return result;
  }

    //解析name
  static String parseName(StringPosition source) {
    if (source.position == source.length) {
      throw new IllegalArgumentException("Missing name at " + source);
    }
    final int start = source.position;
    if (source.value.charAt(source.position) == '`') {//说明name是`包围的
      source.position += 1;
      StringBuilder buffer = new StringBuilder();
      boolean closed = false;
      while (source.position < source.length) {//不断循环---一直找到`才结束
        char ch = source.value.charAt(source.position);//获取一个元素
        source.position += 1;
        if (ch == '`') {//找到`就结束
          if (source.position < source.length &&
              source.value.charAt(source.position) == '`') {//说明是双引号
            source.position += 1;
            buffer.append('`');//添加`,即将双引号变成单引号
          } else {
            closed = true;
            break;
          }
        } else {
          buffer.append(ch);
        }
      }
      if (!closed) {
        source.position = start;
        throw new IllegalArgumentException("Unmatched quote at " + source);
      } else if (buffer.length() == 0) {
        throw new IllegalArgumentException("Empty quoted field name at " + source);
      }
      return buffer.toString();
    } else {//说明name可能没有被`包围
      while (source.position < source.length) {
        char ch = source.value.charAt(source.position);//找到非法的名字结束
        if (!Character.isLetterOrDigit(ch) && ch != '.' && ch != '_') {
          break;
        }
        source.position += 1;
      }
      if (source.position == start) {
        throw new IllegalArgumentException("Missing name at " + source);
      }
      return source.value.substring(start, source.position);
    }
  }

    //要求下一个字符一定是required
  static void requireChar(StringPosition source, char required) {
    if (source.position >= source.length ||
        source.value.charAt(source.position) != required) {
      throw new IllegalArgumentException("Missing required char '" +
          required + "' at " + source);
    }
    source.position += 1;
  }

    //判断下一个字符是否是ch,如果是则位置+1,如果不是则不需要+1
    //返回是否找到该ch
  static boolean consumeChar(StringPosition source, char ch) {
    boolean result = source.position < source.length &&
        source.value.charAt(source.position) == ch;
    if (result) {
      source.position += 1;
    }
    return result;
  }

    //解析union
  static void parseUnion(TypeDescription type, StringPosition source) {
    requireChar(source, '<');
    do {
      type.addUnionChild(parseType(source));
    } while (consumeChar(source, ','));//只要有,则一直要解析
    requireChar(source, '>');
  }

  static void parseStruct(TypeDescription type, StringPosition source) {
    requireChar(source, '<');
    do {
      String fieldName = parseName(source);
      requireChar(source, ':');
      type.addField(fieldName, parseType(source));
    } while (consumeChar(source, ','));
    requireChar(source, '>');
  }

    //真正的解析操作
  static TypeDescription parseType(StringPosition source) {
    TypeDescription result = new TypeDescription(parseCategory(source));
    switch (result.getCategory()) {
      case BINARY:
      case BOOLEAN:
      case BYTE:
      case DATE:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case STRING:
      case TIMESTAMP:
        break;//说明上面的类型已经解析完成
      case CHAR:
      case VARCHAR:
        requireChar(source, '(');//一定下一个字符是(
        result.withMaxLength(parseInt(source));//解析整数
        requireChar(source, ')');//下一个字符一定是)
        break;//说明要解析最大长度
      case DECIMAL: {
        requireChar(source, '(');
        int precision = parseInt(source);//获取一个整数
        requireChar(source, ',');
        result.withScale(parseInt(source));
        result.withPrecision(precision);
        requireChar(source, ')');
        break;
      }
      case LIST: {//List存储了一个数据类型
        requireChar(source, '<');
        TypeDescription child = parseType(source);
        result.children.add(child);
        child.parent = result;
        requireChar(source, '>');
        break;
      }
      case MAP: {//map存储了两个数据类型
        requireChar(source, '<');
        TypeDescription keyType = parseType(source);
        result.children.add(keyType);
        keyType.parent = result;
        requireChar(source, ',');
        TypeDescription valueType = parseType(source);
        result.children.add(valueType);
        valueType.parent = result;
        requireChar(source, '>');
        break;
      }
      case UNION:
        parseUnion(result, source);
        break;
      case STRUCT:
        parseStruct(result, source);
        break;
      default:
        throw new IllegalArgumentException("Unknown type " +
            result.getCategory() + " at " + source);
    }
    return result;
  }

  /**
   * Parse TypeDescription from the Hive type names. This is the inverse
   * of TypeDescription.toString()
   * 给定一个类型内容,就开始解析,相当toString的于反序列化
   * @param typeName the name of the type
   * @return a new TypeDescription or null if typeName was null
   * @throws IllegalArgumentException if the string is badly formed
   */
  public static TypeDescription fromString(String typeName) {
    if (typeName == null) {
      return null;
    }
    StringPosition source = new StringPosition(typeName);
    TypeDescription result = parseType(source);//全部解析完成
    if (source.position != source.length) {//解析后一定保证没有剩余的字节了
      throw new IllegalArgumentException("Extra characters at " + source);
    }
    return result;
  }

  /**
   * For decimal types, set the precision.
   * @param precision the new precision
   * @return this
   */
  public TypeDescription withPrecision(int precision) {
    if (category != Category.DECIMAL) {
      throw new IllegalArgumentException("precision is only allowed on decimal"+
         " and not " + category.name);
    } else if (precision < 1 || precision > MAX_PRECISION || scale > precision){
      throw new IllegalArgumentException("precision " + precision +
          " is out of range 1 .. " + scale);
    }
    this.precision = precision;
    return this;
  }

  /**
   * For decimal types, set the scale.
   * @param scale the new scale
   * @return this
   */
  public TypeDescription withScale(int scale) {
    if (category != Category.DECIMAL) {
      throw new IllegalArgumentException("scale is only allowed on decimal"+
          " and not " + category.name);
    } else if (scale < 0 || scale > MAX_SCALE || scale > precision) {
      throw new IllegalArgumentException("scale is out of range at " + scale);
    }
    this.scale = scale;
    return this;
  }

  public static TypeDescription createVarchar() {
    return new TypeDescription(Category.VARCHAR);
  }

  public static TypeDescription createChar() {
    return new TypeDescription(Category.CHAR);
  }

  /**
   * Set the maximum length for char and varchar types.
   * @param maxLength the maximum value
   * @return this
   */
  public TypeDescription withMaxLength(int maxLength) {
    if (category != Category.VARCHAR && category != Category.CHAR) {
      throw new IllegalArgumentException("maxLength is only allowed on char" +
                   " and varchar and not " + category.name);
    }
    this.maxLength = maxLength;
    return this;
  }

  public static TypeDescription createList(TypeDescription childType) {
    TypeDescription result = new TypeDescription(Category.LIST);
    result.children.add(childType);
    childType.parent = result;
    return result;
  }

  public static TypeDescription createMap(TypeDescription keyType,
                                          TypeDescription valueType) {
    TypeDescription result = new TypeDescription(Category.MAP);
    result.children.add(keyType);
    result.children.add(valueType);
    keyType.parent = result;
    valueType.parent = result;
    return result;
  }

  public static TypeDescription createUnion() {
    return new TypeDescription(Category.UNION);
  }

  public static TypeDescription createStruct() {
    return new TypeDescription(Category.STRUCT);
  }

  /**
   * Add a child to a union type.
   * @param child a new child type to add
   * @return the union type.
   */
  public TypeDescription addUnionChild(TypeDescription child) {
    if (category != Category.UNION) {
      throw new IllegalArgumentException("Can only add types to union type" +
          " and not " + category);
    }
    children.add(child);
    child.parent = this;
    return this;
  }

  /**
   * Add a field to a struct type as it is built.
   * 为struct添加一个field
   * @param field the field name ,field的name
   * @param fieldType the type of the field,field的类型
   * @return the struct type
   */
  public TypeDescription addField(String field, TypeDescription fieldType) {
    if (category != Category.STRUCT) {//只有STRUCT才会调用该方法
      throw new IllegalArgumentException("Can only add fields to struct type" +
          " and not " + category);
    }
    fieldNames.add(field);
    children.add(fieldType);
    fieldType.parent = this;
    return this;
  }

  /**
   * Get the id for this type.
   * The first call will cause all of the the ids in tree to be assigned, so
   * it should not be called before the type is completely built.
   * @return the sequential id
   * 获取本类的ID
   */
  public int getId() {
    // if the id hasn't been assigned, assign all of the ids from the root
    if (id == -1) {
      TypeDescription root = this;//找到根
      while (root.parent != null) {
        root = root.parent;
      }
      root.assignIds(0);//从根开始设置ID
    }
    return id;//获取本类的ID,而不是根的ID
  }

  public TypeDescription clone() {
    TypeDescription result = new TypeDescription(category);
    result.maxLength = maxLength;
    result.precision = precision;
    result.scale = scale;
    if (fieldNames != null) {
      result.fieldNames.addAll(fieldNames);
    }
    if (children != null) {
      for(TypeDescription child: children) {
        TypeDescription clone = child.clone();
        clone.parent = result;
        result.children.add(clone);
      }
    }
    return result;
  }

  @Override
  public int hashCode() {
    long result = category.ordinal() * 4241 + maxLength + precision * 13 + scale;
    if (children != null) {
      for(TypeDescription child: children) {
        result = result * 6959 + child.hashCode();
      }
    }
    return (int) result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof TypeDescription)) {
      return false;
    }
    if (other == this) {
      return true;
    }
    TypeDescription castOther = (TypeDescription) other;
    if (category != castOther.category ||
        maxLength != castOther.maxLength ||
        scale != castOther.scale ||
        precision != castOther.precision) {
      return false;
    }
    if (children != null) {
      if (children.size() != castOther.children.size()) {
        return false;
      }
      for (int i = 0; i < children.size(); ++i) {
        if (!children.get(i).equals(castOther.children.get(i))) {
          return false;
        }
      }
    }
    if (category == Category.STRUCT) {
      for(int i=0; i < fieldNames.size(); ++i) {
        if (!fieldNames.get(i).equals(castOther.fieldNames.get(i))) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Get the maximum id assigned to this type or its children.
   * The first call will cause all of the the ids in tree to be assigned, so
   * it should not be called before the type is completely built.
   * @return the maximum id assigned under this type
   */
  public int getMaximumId() {
    // if the id hasn't been assigned, assign all of the ids from the root
    if (maxId == -1) {
      TypeDescription root = this;//找到最顶层的节点
      while (root.parent != null) {
        root = root.parent;
      }
      root.assignIds(0);//从最顶层的节点开始设置ID,从0开始设置
    }
    return maxId;//返回该类下面最大的ID,而不是整个树最大的ID
  }

  private ColumnVector createColumn(int maxSize) {
    switch (category) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case DATE:
        return new LongColumnVector(maxSize);
      case TIMESTAMP:
        return new TimestampColumnVector(maxSize);
      case FLOAT:
      case DOUBLE:
        return new DoubleColumnVector(maxSize);
      case DECIMAL:
        return new DecimalColumnVector(maxSize, precision, scale);
      case STRING:
      case BINARY:
      case CHAR:
      case VARCHAR:
        return new BytesColumnVector(maxSize);
      case STRUCT: {
        ColumnVector[] fieldVector = new ColumnVector[children.size()];
        for(int i=0; i < fieldVector.length; ++i) {
          fieldVector[i] = children.get(i).createColumn(maxSize);
        }
        return new StructColumnVector(maxSize,
                fieldVector);
      }
      case UNION: {
        ColumnVector[] fieldVector = new ColumnVector[children.size()];
        for(int i=0; i < fieldVector.length; ++i) {
          fieldVector[i] = children.get(i).createColumn(maxSize);
        }
        return new UnionColumnVector(maxSize,
            fieldVector);
      }
      case LIST:
        return new ListColumnVector(maxSize,
            children.get(0).createColumn(maxSize));
      case MAP:
        return new MapColumnVector(maxSize,
            children.get(0).createColumn(maxSize),
            children.get(1).createColumn(maxSize));
      default:
        throw new IllegalArgumentException("Unknown type " + category);
    }
  }

  public VectorizedRowBatch createRowBatch(int maxSize) {
    VectorizedRowBatch result;
    if (category == Category.STRUCT) {
      result = new VectorizedRowBatch(children.size(), maxSize);
      for(int i=0; i < result.cols.length; ++i) {
        result.cols[i] = children.get(i).createColumn(maxSize);
      }
    } else {
      result = new VectorizedRowBatch(1, maxSize);
      result.cols[0] = createColumn(maxSize);
    }
    result.reset();
    return result;
  }

  public VectorizedRowBatch createRowBatch() {
    return createRowBatch(VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Get the kind of this type.
   * @return get the category for this type.
   */
  public Category getCategory() {
    return category;
  }

  /**
   * Get the maximum length of the type. Only used for char and varchar types.
   * @return the maximum length of the string type
   */
  public int getMaxLength() {
    return maxLength;
  }

  /**
   * Get the precision of the decimal type.
   * @return the number of digits for the precision.
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Get the scale of the decimal type.
   * @return the number of digits for the scale.
   */
  public int getScale() {
    return scale;
  }

  /**
   * For struct types, get the list of field names.
   * @return the list of field names.
   */
  public List<String> getFieldNames() {
    return Collections.unmodifiableList(fieldNames);
  }

  /**
   * Get the subtypes of this type.
   * @return the list of children types
   */
  public List<TypeDescription> getChildren() {
    return children == null ? null : Collections.unmodifiableList(children);
  }

  /**
   * Assign ids to all of the nodes under this one.
   * @param startId the lowest id to assign
   * @return the next available id
   * 为所有的节点分配一个id
   */
  private int assignIds(int startId) {
    id = startId++;//本节点ID就是参数id+1
    if (children != null) {//循环所有的子节点
      for (TypeDescription child : children) {
        startId = child.assignIds(startId);//为每一个子节点分配ID
      }
    }
    maxId = startId - 1;//设置该节点下面所有的节点中最大的ID
    return startId;
  }

  public TypeDescription(Category category) {
    this.category = category;
    if (category.isPrimitive) {
      children = null;
    } else {//不是原始类型,因此说明是容器,因此容器需要知道容纳什么类型子元素
      children = new ArrayList<>();
    }
    if (category == Category.STRUCT) {//只有STRUCT才需要有name
      fieldNames = new ArrayList<>();
    } else {
      fieldNames = null;
    }
  }

  private int id = -1;//每一个类型有一个唯一的ID---因为最终是类型树组成的
  private int maxId = -1;//该节点下面最大的ID是多少
  private TypeDescription parent;//类型的依赖关系,即父对象是什么类型
  private final Category category;
  private final List<TypeDescription> children;//包含的类型集合----不是原始类型,因此说明是容器,因此容器需要知道容纳什么类型子元素
  private final List<String> fieldNames;//包含的属性name集合
  private int maxLength = DEFAULT_LENGTH;//最大长度  只有Category.VARCHAR和Category.CHAR才能设置maxLength
  //用于decimal类型
  private int precision = DEFAULT_PRECISION;
  private int scale = DEFAULT_SCALE;

    //打印 name 或者 `name`
  static void printFieldName(StringBuilder buffer, String name) {
    if (UNQUOTED_NAMES.matcher(name).matches()) {//用于查找STRUCT中的name
      buffer.append(name);//追加name
    } else {
      buffer.append('`');
      buffer.append(name.replace("`", "``"));//name中有`的都取消掉
      buffer.append('`');
    }
  }

    //打印类型
  public void printToBuffer(StringBuilder buffer) {
    buffer.append(category.name);
    switch (category) {
      case DECIMAL:
        buffer.append('(');
        buffer.append(precision);
        buffer.append(',');
        buffer.append(scale);
        buffer.append(')');//打印精准度
        break;
      case CHAR:
      case VARCHAR:
        buffer.append('(');
        buffer.append(maxLength);
        buffer.append(')');//打印最大长度
        break;
      case LIST:
      case MAP:
      case UNION:
        buffer.append('<');//打印该对象中每一个元素的类型
        for(int i=0; i < children.size(); ++i) {
          if (i != 0) {
            buffer.append(',');
          }
          children.get(i).printToBuffer(buffer);
        }
        buffer.append('>');
        break;
      case STRUCT:
        buffer.append('<');
        for(int i=0; i < children.size(); ++i) {//打印每一个对象的name以及类型
          if (i != 0) {
            buffer.append(',');
          }
          printFieldName(buffer, fieldNames.get(i));
          buffer.append(':');
          children.get(i).printToBuffer(buffer);
        }
        buffer.append('>');
        break;
      default:
        break;
    }
  }

  public String toString() {
    StringBuilder buffer = new StringBuilder();
    printToBuffer(buffer);
    return buffer.toString();
  }

  private void printJsonToBuffer(String prefix, StringBuilder buffer,
                                 int indent) {
    for(int i=0; i < indent; ++i) {
      buffer.append(' ');
    }
    buffer.append(prefix);
    buffer.append("{\"category\": \"");
    buffer.append(category.name);
    buffer.append("\", \"id\": ");
    buffer.append(getId());
    buffer.append(", \"max\": ");
    buffer.append(maxId);
    switch (category) {
      case DECIMAL:
        buffer.append(", \"precision\": ");
        buffer.append(precision);
        buffer.append(", \"scale\": ");
        buffer.append(scale);
        break;
      case CHAR:
      case VARCHAR:
        buffer.append(", \"length\": ");
        buffer.append(maxLength);
        break;
      case LIST:
      case MAP:
      case UNION:
        buffer.append(", \"children\": [");
        for(int i=0; i < children.size(); ++i) {
          buffer.append('\n');
          children.get(i).printJsonToBuffer("", buffer, indent + 2);
          if (i != children.size() - 1) {
            buffer.append(',');
          }
        }
        buffer.append("]");
        break;
      case STRUCT:
        buffer.append(", \"fields\": [");
        for(int i=0; i < children.size(); ++i) {
          buffer.append('\n');
          children.get(i).printJsonToBuffer("\"" + fieldNames.get(i) + "\": ",
              buffer, indent + 2);
          if (i != children.size() - 1) {
            buffer.append(',');
          }
        }
        buffer.append(']');
        break;
      default:
        break;
    }
    buffer.append('}');
  }

  public String toJson() {
    StringBuilder buffer = new StringBuilder();
    printJsonToBuffer("", buffer, 0);
    return buffer.toString();
  }

  /**
   * 通过一个ID定位到一个对象
   * Locate a subtype by its id.
   * @param goal the column id to look for
   * @return the subtype
   */
  public TypeDescription findSubtype(int goal) {
    // call getId method to make sure the ids are assigned
    int id = getId();//获取本类型ID
    if (goal < id || goal > maxId) {//参数ID一定不能大于maxId,也不能比本类型还小,因为goal要查找的一定是本类ID的子类型
      throw new IllegalArgumentException("Unknown type id " + id + " in " +
          toJson());
    }
    if (goal == id) {//说明就是本类型
      return this;
    } else {
      TypeDescription prev = null;
      for(TypeDescription next: children) {//循环所有的类型
        if (next.id > goal) {//说明类型一定在里面,因此继续查找
          return prev.findSubtype(goal);
        }
        prev = next;
      }
      return prev.findSubtype(goal);
    }
  }
}
