/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift;

import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

/**
 * 应用相关的异常
 * Application level exception
 */
public class TApplicationException extends TException implements TSerializable {
  //应用异常相关的结构、字段定义
  private static final TStruct TAPPLICATION_EXCEPTION_STRUCT = new TStruct("TApplicationException");
  private static final TField MESSAGE_FIELD = new TField("message", TType.STRING, (short)1);
  private static final TField TYPE_FIELD = new TField("type", TType.I32, (short)2);//32位、4字节，int类型的

  private static final long serialVersionUID = 1L;

  //异常类型
  public static final int UNKNOWN = 0;
  public static final int UNKNOWN_METHOD = 1;
  public static final int INVALID_MESSAGE_TYPE = 2;
  public static final int WRONG_METHOD_NAME = 3;
  public static final int BAD_SEQUENCE_ID = 4;
  public static final int MISSING_RESULT = 5;
  public static final int INTERNAL_ERROR = 6;
  public static final int PROTOCOL_ERROR = 7;
  public static final int INVALID_TRANSFORM = 8;
  public static final int INVALID_PROTOCOL = 9;
  public static final int UNSUPPORTED_CLIENT_TYPE = 10;

  protected int type_ = UNKNOWN;
  private String message_ = null;

  public TApplicationException() {
    super();
  }

  public TApplicationException(int type) {
    super();
    type_ = type;
  }

  public TApplicationException(int type, String message) {
    super(message);
    type_ = type;
  }

  public TApplicationException(String message) {
    super(message);
  }

  public int getType() {
    return type_;
  }

  @Override
  public String getMessage() {
    if (message_ == null) {
      return super.getMessage();
    }
    else {
      return message_;
    }
  }

  //fixme 注意，最后从iprot中获取的信息赋值给了当前对象成员变量
  public void read(TProtocol iprot) throws TException {
    //开始读取异常信息
    iprot.readStructBegin();

    TField field;
    String message = null;
    int type = UNKNOWN;

    while (true) {
      //获取到字段的名称、类型信息
      field = iprot.readFieldBegin();
      //如果类型为stop、则break while
      if (field.type == TType.STOP) {
        break;
      }
      // 1：message；2.type； other：skip
      switch (field.id) {
        case 1:
          if (field.type == TType.STRING) {
            //fixme 读取String类型信息的时候，会先获取String的大小、然后读取byte数组
            message = iprot.readString();
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2:
          if (field.type == TType.I32) {
            type = iprot.readI32();
          } else {
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    //fixme
    type_ = type;
    message_ = message;
  }

  // Convenience factory method for constructing a TApplicationException given a TProtocol input
  public static TApplicationException readFrom(TProtocol iprot) throws TException {
    TApplicationException result = new TApplicationException();
    //从指定的协议获取异常信息：read方法包含对 对象成员属性 的赋值
    result.read(iprot);
    return result;
  }

  //将异常信息通过协议响应该远端
  public void write(TProtocol oprot) throws TException {
    oprot.writeStructBegin(TAPPLICATION_EXCEPTION_STRUCT);
    if (getMessage() != null) {
      oprot.writeFieldBegin(MESSAGE_FIELD);
      oprot.writeString(getMessage());
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(TYPE_FIELD);
    oprot.writeI32(type_);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }
}
