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

package org.apache.thrift.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

/**
 * fixme: "thrift的二进制协议实现"
 *
 *    变量：
 *        版本mask;
 *        string长度限制；
 *        从网络中读取容器类(Map、set、list)的最大长度；NO_LENGTH_LIMIT(-1)则表示没限制
 *        是否要进行严格的读写；
 *        8个字节的IO缓存。
 *
 *
 *
 *
 */
public class TBinaryProtocol extends TProtocol {
  private static final TStruct ANONYMOUS_STRUCT = new TStruct();
  private static final long NO_LENGTH_LIMIT = -1;

  /**
   * 0x开始表示16进制：
   *    0xffff0000 = 1111111111111111 0000000000000000  //16个1、16个0
   *    0x80010000 = 1000000000000001 0000000000000000  //1+(14个0)+1+16个0
   */
  protected static final int VERSION_MASK = 0xffff0000;
  protected static final int VERSION_1 = 0x80010000;

  /**
   * 当读取的数据为可变长度时、例如String或者二进制数据，
   * stringLengthLimit_ 表示可以从传输层读取的最大长度， NO_LENGTH_LIMIT (-1)表示没有限制。
   */
  private final long stringLengthLimit_;

  /**
   * 从网络中读取容器类(Map、set、list)的最大长度；NO_LENGTH_LIMIT(-1)则表示没限制；
   */
  private final long containerLengthLimit_;

  //是否要进行严格的读写
  protected boolean strictRead_;
  protected boolean strictWrite_;

  //8个字节
  private final byte[] inoutTemp = new byte[8];

  /**
   * Factory
   */
  public static class Factory implements TProtocolFactory {
    //限制的长度
    protected long stringLengthLimit_;
    protected long containerLengthLimit_;

    //严格读取？
    protected boolean strictRead_;
    //严格写入？
    protected boolean strictWrite_;

    //读宽松、写严格
    public Factory() {
      this(false, true);
    }

    public Factory(boolean strictRead, boolean strictWrite) {
      //-1表示长度没有限制
      this(strictRead, strictWrite, NO_LENGTH_LIMIT, NO_LENGTH_LIMIT);
    }

    public Factory(long stringLengthLimit, long containerLengthLimit) {
      //也是默认 读宽松、写严格
      this(false, true, stringLengthLimit, containerLengthLimit);
    }

    public Factory(boolean strictRead, boolean strictWrite, long stringLengthLimit, long containerLengthLimit) {
      stringLengthLimit_ = stringLengthLimit;
      containerLengthLimit_ = containerLengthLimit;
      strictRead_ = strictRead;
      strictWrite_ = strictWrite;
    }

    //fixme 默认读宽松、写严格，长度没有限制
    public TProtocol getProtocol(TTransport trans) {
      return new TBinaryProtocol(trans, stringLengthLimit_, containerLengthLimit_, strictRead_, strictWrite_);
    }
  }

  /**
   * Constructor
   */
  public TBinaryProtocol(TTransport trans) {
    this(trans, false, true);
  }

  public TBinaryProtocol(TTransport trans, boolean strictRead, boolean strictWrite) {
    this(trans, NO_LENGTH_LIMIT, NO_LENGTH_LIMIT, strictRead, strictWrite);
  }

  public TBinaryProtocol(TTransport trans, long stringLengthLimit, long containerLengthLimit) {
    this(trans, stringLengthLimit, containerLengthLimit, false, true);
  }

  public TBinaryProtocol(TTransport trans, long stringLengthLimit, long containerLengthLimit, boolean strictRead, boolean strictWrite) {
    super(trans);
    stringLengthLimit_ = stringLengthLimit;
    containerLengthLimit_ = containerLengthLimit;
    strictRead_ = strictRead;
    strictWrite_ = strictWrite;
  }

  // 将方法名称、类型和序列id等写入到目标buf/流中，在向远端传递参数数据之前调用
  @Override
  public void writeMessageBegin(TMessage message) throws TException {
    //严格的写
    if (strictWrite_) {

      //向远端传输当前的消息类型：CALL、REPLY、EXCEPTION、ONEWAY
      int version = VERSION_1 | message.type;
      writeI32(version);

      //向远端传输当前的方法名称
      writeString(message.name);

      //向远端传输当前调用的序列ID
      writeI32(message.seqid);
    } else {

      //向远端传递方法名称、类型和调用序列ID
      writeString(message.name);
      writeByte(message.type);
      writeI32(message.seqid);
    }
  }

  @Override
  public void writeMessageEnd() throws TException {}

  @Override
  public void writeStructBegin(TStruct struct) throws TException {}

  @Override
  public void writeStructEnd() throws TException {}

  @Override
  public void writeFieldBegin(TField field) throws TException {
    writeByte(field.type);
    writeI16(field.id);
  }

  @Override
  public void writeFieldEnd() throws TException {}

  @Override
  public void writeFieldStop() throws TException {
    writeByte(TType.STOP);
  }

  @Override
  public void writeMapBegin(TMap map) throws TException {
    writeByte(map.keyType);
    writeByte(map.valueType);
    writeI32(map.size);
  }

  @Override
  public void writeMapEnd() throws TException {}

  @Override
  public void writeListBegin(TList list) throws TException {
    writeByte(list.elemType);
    writeI32(list.size);
  }

  @Override
  public void writeListEnd() throws TException {}

  @Override
  public void writeSetBegin(TSet set) throws TException {
    writeByte(set.elemType);
    writeI32(set.size);
  }

  @Override
  public void writeSetEnd() throws TException {}

  //boolean类型数据通过byte写入
  @Override
  public void writeBool(boolean b) throws TException {
    writeByte(b ? (byte)1 : (byte)0);
  }

  //写一个byte、1字节的数据到 流/缓存区/socket 中
  @Override
  public void writeByte(byte b) throws TException {
    inoutTemp[0] = b;
    trans_.write(inoutTemp, 0, 1);
  }

  //写一个short类型数据、2字节到 流/缓存区/socket 中
  @Override
  public void writeI16(short i16) throws TException {
    inoutTemp[0] = (byte)(0xff & (i16 >> 8));
    inoutTemp[1] = (byte)(0xff & (i16));
    trans_.write(inoutTemp, 0, 2);
  }

  //写32bit数据、即4字节的int类型数据到 流/缓存区/socket 中
  @Override
  public void writeI32(int i32) throws TException {
    inoutTemp[0] = (byte)(0xff & (i32 >> 24));
    inoutTemp[1] = (byte)(0xff & (i32 >> 16));
    inoutTemp[2] = (byte)(0xff & (i32 >> 8));
    inoutTemp[3] = (byte)(0xff & (i32));
    trans_.write(inoutTemp, 0, 4);
  }

  //写64bit数据、即8字节的long类型数据到 流/缓存区/socket 中
  @Override
  public void writeI64(long i64) throws TException {
    inoutTemp[0] = (byte)(0xff & (i64 >> 56));
    inoutTemp[1] = (byte)(0xff & (i64 >> 48));
    inoutTemp[2] = (byte)(0xff & (i64 >> 40));
    inoutTemp[3] = (byte)(0xff & (i64 >> 32));
    inoutTemp[4] = (byte)(0xff & (i64 >> 24));
    inoutTemp[5] = (byte)(0xff & (i64 >> 16));
    inoutTemp[6] = (byte)(0xff & (i64 >> 8));
    inoutTemp[7] = (byte)(0xff & (i64));
    trans_.write(inoutTemp, 0, 8);
  }

  @Override
  public void writeDouble(double dub) throws TException {
    writeI64(Double.doubleToLongBits(dub));
  }

  @Override
  public void writeString(String str) throws TException {
    byte[] dat = str.getBytes(StandardCharsets.UTF_8);
    writeI32(dat.length);
    trans_.write(dat, 0, dat.length);
  }

  @Override
  public void writeBinary(ByteBuffer bin) throws TException {
    int length = bin.limit() - bin.position();
    writeI32(length);
    trans_.write(bin.array(), bin.position() + bin.arrayOffset(), length);
  }

  /**
   * Reading methods.
   */

  @Override
  public TMessage readMessageBegin() throws TException {
    //fixme 读取8个字节的int类型数据：要读取的数据大小？？
    int size = readI32();

    //如果要读取的数据小于0
    if (size < 0) {
      int version = size & VERSION_MASK;
      if (version != VERSION_1) {
        throw new TProtocolException(TProtocolException.BAD_VERSION, "Bad version in readMessageBegin");
      }
      return new TMessage(readString(), (byte)(size & 0x000000ff), readI32());
    } else {
      //如果是严格读、"在readMessageBegin中丢失了version，旧版本client？"
      if (strictRead_) {
        throw new TProtocolException(TProtocolException.BAD_VERSION, "Missing version in readMessageBegin, old client?");
      }
      //
      return new TMessage(readStringBody(size), readByte(), readI32());
    }
  }

  @Override
  public void readMessageEnd() throws TException {}

  @Override
  public TStruct readStructBegin() throws TException {
    return ANONYMOUS_STRUCT;
  }

  @Override
  public void readStructEnd() throws TException {}

  @Override
  public TField readFieldBegin() throws TException {
    //读取的字段是什么类型的
    byte type = readByte();
    short id = type == TType.STOP ? 0 : readI16();
    return new TField("", type, id);
  }

  @Override
  public void readFieldEnd() throws TException {}

  @Override
  public TMap readMapBegin() throws TException {
    //keyType, valueType, size
    TMap map = new TMap(readByte(), readByte(), readI32());
    //如果容器长度超过限制、则抛异常
    checkContainerReadLength(map.size);
    return map;
  }

  @Override
  public void readMapEnd() throws TException {}

  @Override
  public TList readListBegin() throws TException {
    TList list = new TList(readByte(), readI32());
    checkContainerReadLength(list.size);
    return list;
  }

  @Override
  public void readListEnd() throws TException {}

  @Override
  public TSet readSetBegin() throws TException {
    TSet set = new TSet(readByte(), readI32());
    checkContainerReadLength(set.size);
    return set;
  }

  @Override
  public void readSetEnd() throws TException {}

  @Override
  public boolean readBool() throws TException {
    return (readByte() == 1);
  }

  @Override
  public byte readByte() throws TException {
    //fixme 缓存区剩余可读取的字节数不为空
    if (trans_.getBytesRemainingInBuffer() >= 1) {
      //你细品，获取的就是指针所在的元素
      byte b = trans_.getBuffer()[trans_.getBufferPosition()];
      //指针后移
      trans_.consumeBuffer(1);
      //返回读取的字节数
      return b;
    }

    readAll(inoutTemp, 0, 1);
    return inoutTemp[0];
  }

  @Override
  public short readI16() throws TException {
    byte[] buf = inoutTemp;
    int off = 0;

    if (trans_.getBytesRemainingInBuffer() >= 2) {
      buf = trans_.getBuffer();
      off = trans_.getBufferPosition();
      trans_.consumeBuffer(2);
    } else {
      readAll(inoutTemp, 0, 2);
    }

    return
      (short)
      (((buf[off] & 0xff) << 8) |
       ((buf[off+1] & 0xff)));
  }

  @Override
  public int readI32() throws TException {
    //8字节、int型
    byte[] buf = inoutTemp;
    //偏移量
    int off = 0;

    //trans_：该协议层的传输工具对象
    //如果可读取的数据大于4字节
    if (trans_.getBytesRemainingInBuffer() >= 4) {
      //获取缓冲区数据
      buf = trans_.getBuffer();
      //获取缓存区当前读取位置的偏移量
      off = trans_.getBufferPosition();
      //偏移量递增4
      trans_.consumeBuffer(4);
    } else {
      readAll(inoutTemp, 0, 4);
    }
    //返回int值
    return
      ((buf[off] & 0xff) << 24) |
      ((buf[off+1] & 0xff) << 16) |
      ((buf[off+2] & 0xff) <<  8) |
      ((buf[off+3] & 0xff));
  }

  @Override
  public long readI64() throws TException {
    byte[] buf = inoutTemp;
    int off = 0;

    if (trans_.getBytesRemainingInBuffer() >= 8) {
      buf = trans_.getBuffer();
      off = trans_.getBufferPosition();
      trans_.consumeBuffer(8);
    } else {
      readAll(inoutTemp, 0, 8);
    }

    return
      ((long)(buf[off]   & 0xff) << 56) |
      ((long)(buf[off+1] & 0xff) << 48) |
      ((long)(buf[off+2] & 0xff) << 40) |
      ((long)(buf[off+3] & 0xff) << 32) |
      ((long)(buf[off+4] & 0xff) << 24) |
      ((long)(buf[off+5] & 0xff) << 16) |
      ((long)(buf[off+6] & 0xff) <<  8) |
      ((long)(buf[off+7] & 0xff));
  }

  @Override
  public double readDouble() throws TException {
    return Double.longBitsToDouble(readI64());
  }

  @Override
  public String readString() throws TException {
    int size = readI32();

    checkStringReadLength(size);

    if (trans_.getBytesRemainingInBuffer() >= size) {
      String s = new String(trans_.getBuffer(), trans_.getBufferPosition(),
          size, StandardCharsets.UTF_8);
      trans_.consumeBuffer(size);
      return s;
    }

    return readStringBody(size);
  }

  //fixme 读取指定大小的String字符串？
  public String readStringBody(int size) throws TException {
    checkStringReadLength(size);
    byte[] buf = new byte[size];
    trans_.readAll(buf, 0, size);
    return new String(buf, StandardCharsets.UTF_8);
  }

  //从远端/传输层读取二进制数据
  @Override
  public ByteBuffer readBinary() throws TException {
    //要读取的数据长度
    int size = readI32();

    //校验要读取的长度是否合法：超过最长限制
    checkStringReadLength(size);

    //如果传输层有超过要读取长度的数据、则读取指定长度的数据
    if (trans_.getBytesRemainingInBuffer() >= size) {
      //获取缓存、读取偏移量和缓存大小：remainSize就是 大小-偏移量
      //ByteBuffer.wrap(baseBuff,baseBuff偏移量,复制的数据量)
      ByteBuffer bb = ByteBuffer.wrap(trans_.getBuffer(), trans_.getBufferPosition(), size);
      trans_.consumeBuffer(size);
      return bb;
    }

    //如果传输层要读取的数据大小不超过
    byte[] buf = new byte[size];
    trans_.readAll(buf, 0, size);
    return ByteBuffer.wrap(buf);
  }

  /**
   * 检查读取的 String、byte长度是否合法
   */
  private void checkStringReadLength(int length) throws TProtocolException {
    //负数大小、异常
    if (length < 0) {
      throw new TProtocolException(TProtocolException.NEGATIVE_SIZE, "Negative length: " + length);
    }
    //如果不是无限制的读取、并且长度超过最大限制，抛异常
    if (stringLengthLimit_ != NO_LENGTH_LIMIT && length > stringLengthLimit_) {
      throw new TProtocolException(TProtocolException.SIZE_LIMIT,
                                   "Length exceeded max allowed: " + length);
    }
  }


  //如果容器长度超过限制、则抛异常
  private void checkContainerReadLength(int length) throws TProtocolException {
    if (length < 0) {
      throw new TProtocolException(TProtocolException.NEGATIVE_SIZE, "Negative length: " + length);
    }

    if (containerLengthLimit_ != NO_LENGTH_LIMIT && length > containerLengthLimit_) {
      throw new TProtocolException(TProtocolException.SIZE_LIMIT, "Length exceeded max allowed: " + length);
    }
  }

  //从传输层读取len的数据到buf中，存放起始偏移量为off
  //fixme 该方法是传输层的数据不够所要读取的类型时、采用轮训的方式读取的。
  private int readAll(byte[] buf, int off, int len) throws TException {
    return trans_.readAll(buf, off, len);
  }
}
