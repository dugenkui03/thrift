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
package org.apache.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

/**
 * 字段枚举标识、对应的字段枚举值
 */
public abstract class TUnion<
                              T extends TUnion<T,F>,
                              F extends TFieldIdEnum
                            >
        implements TBase<T, F> {

  protected Object value_;
  protected F setField_;

  protected TUnion() {
    setField_ = null;
    value_ = null;
  }

  //fixme ISchema.class 对应的工厂类
  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TUnionStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TUnionTupleSchemeFactory());
  }

  //将setField标识的字段赋值为value
  protected TUnion(F setField, Object value) {
    setFieldValue(setField, value);
  }

  //使用其他的TUnion作为构造函数
  protected TUnion(TUnion<T, F> other) {
    if (!other.getClass().equals(this.getClass())) {
      throw new ClassCastException();
    }
    setField_ = other.setField_;
    value_ = deepCopyObject(other.value_);
  }


  private static Object deepCopyObject(Object o) {
    if (o instanceof TBase) {
      return ((TBase)o).deepCopy();
    } else if (o instanceof ByteBuffer) {
      return TBaseHelper.copyBinary((ByteBuffer)o);
    } else if (o instanceof List) {
      return deepCopyList((List)o);
    } else if (o instanceof Set) {
      return deepCopySet((Set)o);
    } else if (o instanceof Map) {
      return deepCopyMap((Map)o);
    } else {
      return o;
    }
  }

  private static Map deepCopyMap(Map<Object, Object> map) {
    Map copy = new HashMap(map.size());
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      copy.put(deepCopyObject(entry.getKey()), deepCopyObject(entry.getValue()));
    }
    return copy;
  }

  private static Set deepCopySet(Set set) {
    Set copy = new HashSet(set.size());
    for (Object o : set) {
      copy.add(deepCopyObject(o));
    }
    return copy;
  }

  private static List deepCopyList(List list) {
    List copy = new ArrayList(list.size());
    for (Object o : list) {
      copy.add(deepCopyObject(o));
    }
    return copy;
  }

  public F getSetField() {
    return setField_;
  }

  public Object getFieldValue() {
    return value_;
  }

  public Object getFieldValue(F fieldId) {
    if (fieldId != setField_) {
      throw new IllegalArgumentException("Cannot get the value of field " + fieldId + " because union's set field is " + setField_);
    }

    return getFieldValue();
  }

  public Object getFieldValue(int fieldId) {
    return getFieldValue(enumForId((short)fieldId));
  }

  public boolean isSet() {
    return setField_ != null;
  }

  public boolean isSet(F fieldId) {
    return setField_ == fieldId;
  }

  public boolean isSet(int fieldId) {
    return isSet(enumForId((short)fieldId));
  }

  public void read(TProtocol iprot) throws TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void setFieldValue(F fieldId, Object value) {
    checkType(fieldId, value);
    setField_ = fieldId;
    value_ = value;
  }

  public void setFieldValue(int fieldId, Object value) {
    setFieldValue(enumForId((short)fieldId), value);
  }

  public void write(TProtocol oprot) throws TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  //Implementation should be generated so that we can efficiently type check various values.
  //相关代码应该使用编译器生成、检查必写参数
  protected abstract void checkType(F setField, Object value) throws ClassCastException;


  //使用指定协议读取指定字段；使用指定协议写入字段值
  protected abstract Object standardSchemeReadValue(TProtocol iprot, TField field) throws TException;
  protected abstract void standardSchemeWriteValue(TProtocol oprot) throws TException;

  //同上，但是tupleScheme
  protected abstract Object tupleSchemeReadValue(TProtocol iprot, short fieldID) throws TException;
  protected abstract void tupleSchemeWriteValue(TProtocol oprot) throws TException;

  //只有一个类名称属性
  protected abstract TStruct getStructDesc();

  //属性名称、类型和序列id
  protected abstract TField getFieldDesc(F setField);

  protected abstract F enumForId(short id);

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("<");
    sb.append(this.getClass().getSimpleName());
    sb.append(" ");

    if (getSetField() != null) {
      Object v = getFieldValue();
      sb.append(getFieldDesc(getSetField()).name);
      sb.append(":");
      if(v instanceof ByteBuffer) {
        TBaseHelper.toString((ByteBuffer)v, sb);
      } else {
        sb.append(v.toString());
      }
    }
    sb.append(">");
    return sb.toString();
  }

  public final void clear() {
    this.setField_ = null;
    this.value_ = null;
  }

  private static class TUnionStandardSchemeFactory implements SchemeFactory {
    public TUnionStandardScheme getScheme() {
      return new TUnionStandardScheme();
    }
  }

  private static class TUnionStandardScheme extends StandardScheme<TUnion> {

    @Override
    public void read(TProtocol iprot, TUnion struct) throws TException {
      struct.setField_ = null;
      struct.value_ = null;

      /**
       * 开始读取 自定义类
       */
      iprot.readStructBegin();

      /**
       * 开始读取字段
       */
      TField field = iprot.readFieldBegin();

      struct.value_ = struct.standardSchemeReadValue(iprot, field);
      if (struct.value_ != null) {
        struct.setField_ = struct.enumForId(field.id);
      }

      iprot.readFieldEnd();
      // this is so that we will eat the stop byte. we could put a check here to
      // make sure that it actually *is* the stop byte, but it's faster to do it
      // this way.
      iprot.readFieldBegin();
      iprot.readStructEnd();
    }

    @Override
    public void write(TProtocol oprot, TUnion struct) throws TException {
      if (struct.getSetField() == null || struct.getFieldValue() == null) {
        throw new TProtocolException("Cannot write a TUnion with no set value!");
      }
      oprot.writeStructBegin(struct.getStructDesc());
      oprot.writeFieldBegin(struct.getFieldDesc(struct.setField_));
      struct.standardSchemeWriteValue(oprot);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }
  }

  private static class TUnionTupleSchemeFactory implements SchemeFactory {
    public TUnionTupleScheme getScheme() {
      return new TUnionTupleScheme();
    }
  }

  private static class TUnionTupleScheme extends TupleScheme<TUnion> {

    @Override
    public void read(TProtocol iprot, TUnion struct) throws TException {
      struct.setField_ = null;
      struct.value_ = null;
      short fieldID = iprot.readI16();
      struct.value_ = struct.tupleSchemeReadValue(iprot, fieldID);
      if (struct.value_ != null) {
        struct.setField_ = struct.enumForId(fieldID);
      }
    }

    @Override
    public void write(TProtocol oprot, TUnion struct) throws TException {
      if (struct.getSetField() == null || struct.getFieldValue() == null) {
        throw new TProtocolException("Cannot write a TUnion with no set value!");
      }
      oprot.writeI16(struct.setField_.getThriftFieldId());
      struct.tupleSchemeWriteValue(oprot);
    }
  }
}
