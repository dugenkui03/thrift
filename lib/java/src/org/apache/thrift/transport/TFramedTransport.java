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

package org.apache.thrift.transport;

import org.apache.thrift.TByteArrayOutputStream;

/**
 * TFramedTransport是个缓存传输层，通过4字节的帧头标识传输数据大小、来保证读取的完整性。
 *
 * TFramedTransport is a buffered TTransport that ensures a fully read
 * message every time by preceding messages with a 4-byte frame size.
 */
public class TFramedTransport extends TTransport {

  //要读取数据的最大长度和使用的默认最大长度
  private int maxLength_;

  //二进制的 1111_1010_0000_#_0000_0000_0000
  protected static final int DEFAULT_MAX_LENGTH = 16384000;

  //fixme 底层的传输层：继承+组合
  private TTransport transport_ = null;

  //输出缓存/写缓存
  private final TByteArrayOutputStream writeBuffer_ = new TByteArrayOutputStream(1024);

  //读取缓存/输入缓存
  private final TMemoryInputTransport readBuffer_ = new TMemoryInputTransport(new byte[0]);

  //比通用传输层只多一个maxLength_属性
  public static class Factory extends TTransportFactory {
    private int maxLength_;

    public Factory() {
      maxLength_ = TFramedTransport.DEFAULT_MAX_LENGTH;
    }

    public Factory(int maxLength) {
      maxLength_ = maxLength;
    }

    @Override
    public TTransport getTransport(TTransport base) {
      return new TFramedTransport(base, maxLength_);
    }
  }

  /**
   * Something to fill in the first four bytes of the buffer to make room for the frame size.
   * This allows the implementation to write once instead of twice.
   *
   *  to make room for ： 为......腾出空间
   */
  private static final byte[] sizeFiller_ = new byte[] { 0x00, 0x00, 0x00, 0x00 };

  /**
   * 使用另外一个传输层和 指定/默认 最大传输长度构造帧传输层。
   */
  public TFramedTransport(TTransport transport, int maxLength) {
    transport_ = transport;
    maxLength_ = maxLength;
    writeBuffer_.write(sizeFiller_, 0, 4);
  }

  public TFramedTransport(TTransport transport) {
    transport_ = transport;
    maxLength_ = TFramedTransport.DEFAULT_MAX_LENGTH;
    writeBuffer_.write(sizeFiller_, 0, 4);
  }

  //打开传输层连接
  public void open() throws TTransportException {
    transport_.open();
  }

  //传输层是否打开状态
  public boolean isOpen() {
    return transport_.isOpen();
  }

  //关闭传输层
  public void close() {
    transport_.close();
  }

  //todo 从远端读取数据到buf中
  public int read(byte[] buf, int off, int len) throws TTransportException {
    int got = readBuffer_.read(buf, off, len);
    if (got > 0) {
      return got;
    }

    // Read another frame of data
    //todo 读取另一帧数据
    readFrame();

    return readBuffer_.read(buf, off, len);
  }

  //获取底层的缓存
  @Override
  public byte[] getBuffer() {
    return readBuffer_.getBuffer();
  }

  //获取指针当前的位置
  @Override
  public int getBufferPosition() {
    return readBuffer_.getBufferPosition();
  }

  //获取传输层可获取的数据长度
  @Override
  public int getBytesRemainingInBuffer() {
    return readBuffer_.getBytesRemainingInBuffer();
  }

  //从底层缓存读取len个字节
  @Override
  public void consumeBuffer(int len) {
    readBuffer_.consumeBuffer(len);
  }

  //清除底层缓存：buf_ = null;
  public void clear() {
    readBuffer_.clear();
  }

  //32 byte数据的缓存？
  private final byte[] i32buf = new byte[4];

  //todo 读取
  private void readFrame() throws TTransportException {
    //fixme 获取要读取帧的大小
    //读取4个字节的数据到i32buf
    transport_.readAll(i32buf, 0, 4);
    //将4 byte数组解析为int类型、表示帧大小
    int size = decodeFrameSize(i32buf);

    //fixme 检查读取到的帧大小
    //读取到的数据小于0、则关闭传输层并抛异常
    if (size < 0) {
      close();
      throw new TTransportException(TTransportException.CORRUPTED_DATA, "Read a negative frame size (" + size + ")!");
    }

    //读取到的数据大于最大长度、也关闭传输层比个抛异常
    if (size > maxLength_) {
      close();
      throw new TTransportException(TTransportException.CORRUPTED_DATA,
          "Frame size (" + size + ") larger than max length (" + maxLength_ + ")!");
    }

    //将数据读取到buf、并且将传输层缓存重置为buff
    byte[] buff = new byte[size];
    transport_.readAll(buff, 0, size);
    //将传输层缓存重置为buff
    readBuffer_.reset(buff);
  }

  //将buf中、off开始的len长度的数据写入到 写缓存 中
  public void write(byte[] buf, int off, int len) throws TTransportException {
    writeBuffer_.write(buf, off, len);
  }

  //flush 清除缓存
  @Override
  public void flush() throws TTransportException {
    byte[] buf = writeBuffer_.get();

    // account for the prepended frame size
    // 因为有4byte数据标识数据大小，并不缓存数据
    int len = writeBuffer_.len() - 4;
    writeBuffer_.reset();
    // make room for the next frame's size data
    //为下一帧 "数据大小" 存储区
    writeBuffer_.write(sizeFiller_, 0, 4);

    // this is the frame length without the filler
    // 没有filter的帧长度
    encodeFrameSize(len, buf);
    transport_.write(buf, 0, len + 4);      // we have to write the frame size and frame data
    transport_.flush();
  }

  /**
   * 将帧大小信息frameSize(整型值)存放到存放到byte数组中
   *
   * 一个整型、4个字节，正好。高位存放在索引序号低的地方。
   */
  public static final void encodeFrameSize(final int frameSize, final byte[] buf) {
    buf[0] = (byte)(0xff & (frameSize >> 24));
    buf[1] = (byte)(0xff & (frameSize >> 16));
    buf[2] = (byte)(0xff & (frameSize >> 8));
    buf[3] = (byte)(0xff & (frameSize));
  }

  //同encodeFrameSize，将byte数组中存放的、帧大小数据解析为int值
  public static final int decodeFrameSize(final byte[] buf) {
    return
      ((buf[0] & 0xff) << 24) |
      ((buf[1] & 0xff) << 16) |
      ((buf[2] & 0xff) <<  8) |
      ((buf[3] & 0xff));
  }
}
