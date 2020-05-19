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

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;

/**
 *
 *  工具类：将Java序列化为byte或者java类的工具
 *
 * Generic utility for easily serializing objects into a byte array or Java String.
 */
public class TSerializer {

  /**
   * 对象序列化的字节数组，被transport_包装
   *
   * This is the byte array that data is actually serialized into
   */
  private final ByteArrayOutputStream baos_ = new ByteArrayOutputStream();

  /**
   * 这个传输器包装了字节数组，被TProtocol包装
   *
   * This transport wraps that byte array
   */
  private final TIOStreamTransport transport_ = new TIOStreamTransport(baos_);

  /**
   * fixme 组合.接口的方式。这是什么模式
   *
   * 序列化对象的协议
   *
   * Internal protocol used for serializing objects.
   */
  private TProtocol protocol_;

  /**
   * 构造函数：创建一个序列化器、默认使用二进制协议。
   *
   * Create a new TSerializer that uses the TBinaryProtocol by default.
   */
  public TSerializer() {
    this(new TBinaryProtocol.Factory());
  }

  /**
   * 使用工厂协议创建一个序列化器
   *
   * Create a new TSerializer. It will use the TProtocol specified by the factory that is passed in.
   *
   * @param protocolFactory Factory to create a protocol 创建协议的工厂
   *                        工程模式：创建对象时不会对客户端暴露创建逻辑
   */
  public TSerializer(TProtocolFactory protocolFactory) {
    protocol_ = protocolFactory.getProtocol(transport_);
  }

  /**
   * fixme:
   *    将Thrift对象序列化为字节数组。
   *    该过程很简单，只需清除字节数组输出，将对象写入其中，然后获取原始字节即可。
   *
   * Serialize the Thrift object into a byte array. The process is simple,
   * just clear the byte array output, write the object into it, and grab the
   * raw bytes.
   *
   * @param base The object to serialize
   * @return Serialized object in byte[] format
   */
  public byte[] serialize(TBase base) throws TException {
    baos_.reset();

    /**
     * protocol_，通过transport_、保存了baos_的值
     */
    base.write(protocol_);
    return baos_.toByteArray();
  }

  /**
   * 使用自定的字符集、将Thrift对象序列化为字符集合。
   *
   * Serialize the Thrift object into a Java string, using a specified character set for encoding.
   *
   * @param base The object to serialize 要序列化的对象
   * @param charset Valid JVM charset 字符集
   *
   * @return Serialized object as a String 序列化对象为String。
   */
  public String toString(TBase base, String charset) throws TException {
    try {
      return new String(serialize(base), charset);
    } catch (UnsupportedEncodingException uex) {
      throw new TException("JVM DOES NOT SUPPORT ENCODING: " + charset);
    }
  }

  /**
   * 使用JVM默认的编码，将对象序列化为字符串。
   *
   * Serialize the Thrift object into a Java string, using the default JVM charset encoding.
   *
   * @param base The object to serialize
   * @return Serialized object as a String
   */
  public String toString(TBase base) throws TException {
    return new String(serialize(base));
  }
}

