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

  //二进制的 1111_1010 # 0000_0000_0000_0000
  protected static final int DEFAULT_MAX_LENGTH = 16384000;

  //fixme 底层的传输层：继承+组合
  private TTransport transport_ = null;

  //fixme 输出流：比父类ByteArrayOutputStream多了三个语法糖方法：
  //      获取byte数组 buf 的getter；buf的大小、清空buf(buf = new byte[initialSize])
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

  /**fixme 标识缓冲区大小？一直都00 0x00，与运算为0、或运算保持不变
   * Something to fill in the first four bytes of the buffer to make room for the frame size.
   * This allows the implementation to write once instead of twice.
   *
   *  to make room for ： 为......腾出空间
   */
  private static final byte[] sizeFiller_ = new byte[] { 0x00, 0x00, 0x00, 0x00 };

  //使用另外一个传输层和 指定/默认 最大传输长度构造帧传输层。
  public TFramedTransport(TTransport transport, int maxLength) {
    transport_ = transport;
    maxLength_ = maxLength;
    //将sizeFiller_的前4个字节写入到输出流
    writeBuffer_.write(sizeFiller_, 0, 4);
  }

  public TFramedTransport(TTransport transport) {
    transport_ = transport;
    maxLength_ = TFramedTransport.DEFAULT_MAX_LENGTH;
    //将sizeFiller_的前4个字节写入到输出流
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

  /**
   * 从输入流读取数据到buf中
   * @param buf Array to read into 存储数据的数组
   * @param off Index to start reading at 在buf中开始存储数据的偏移量
   * @param len Maximum number of bytes to read 期望读取的最大字节数量
   */
  public int read(byte[] buf, int off, int len) throws TTransportException {
    //从输入流读取最多len的数据到buf中、存放位置从off开始。fixme 返回实际读取到的数据长度
    int got = readBuffer_.read(buf, off, len);
    //读取到数据则返回
    if (got > 0) {
      return got;
    }

    //如果从输入流中没有读取到数据、表示输入流中没有数据了(getBytesRemainingInBuffer = endPos_ - pos_ = 0)
    // Read another frame of data
    readFrame();

    // 在执行"readBuffer_.read(buf, off, len)"、从输入流中读取数据
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

  private void readFrame() throws TTransportException {
    //从传输层读取32bit、4字节大小的数据到i32buf中、表示要获取的数据的大小？
    transport_.readAll(i32buf, 0, 4);
    int size = decodeFrameSize(i32buf);

    /**
     * 要读取到的数据小于0、或者超过阈值、则关闭传输层并抛异常
     */
    if (size < 0) {
      close();//关闭传输层
      throw new TTransportException(TTransportException.CORRUPTED_DATA, "Read a negative frame size (" + size + ")!");
    }

    if (size > maxLength_) {
      close();//关闭传输层
      throw new TTransportException(TTransportException.CORRUPTED_DATA, "Frame size (" + size + ") larger than max length (" + maxLength_ + ")!");
    }

    //fixme 将数据读取到buf、并且将传输层缓存重置为buff
    byte[] buff = new byte[size];
    transport_.readAll(buff, 0, size);
    //将传输层缓存重置为buff
    readBuffer_.reset(buff);
  }

  //将buf中、off开始的len长度的数据写入到 写缓存 中
  public void write(byte[] buf, int off, int len) throws TTransportException {
    writeBuffer_.write(buf, off, len);
  }

  /**
   * fixme:
   *    1. 清空底层缓存数据；
   *    2. 将要清空的数据通过协议写向远端。
   */
  @Override
  public void flush() throws TTransportException {
    //获取底层缓存
    byte[] buf = writeBuffer_.get();

    // "account for the prepended frame size" todo 缓存区字节大小减去4；
    int len = writeBuffer_.len() - 4;

    //将writeBuffer底层缓存数组重置为空数组、fixme 注意，buf指向的数组仍然是存放着数据的
    writeBuffer_.reset();

    //"make room for the next frame's size data" 将sizeFiller_中的4个字节写入到输出流
    writeBuffer_.write(sizeFiller_, 0, 4);

    // "this is the frame length without the filler"
    // fixme 将"没有filter的帧长度"放到buf前四位字节数组中
    encodeFrameSize(len, buf);

    // we have to write the frame size and frame data
    // 将要清空的数据写向远端
    transport_.write(buf, 0, len + 4);
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
