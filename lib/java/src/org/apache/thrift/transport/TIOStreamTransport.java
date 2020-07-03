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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 最常用的基本传输：使用一个输入/输出流作为属性，进行传输操作。
 *
 *
 * This is the most commonly used base transport. It takes an InputStream or
 * an OutputStream or both and uses it/them to perform transport operations.
 *
 * ？？
 * This allows for compatibility(兼容性) with all the nice constructs Java already
 * has to provide a variety of(各种各样的) types of streams.
 *
 */
public class TIOStreamTransport extends TTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TIOStreamTransport.class.getName());

  /** Underlying inputStream */
  protected InputStream inputStream_ = null;

  /** Underlying outputStream */
  protected OutputStream outputStream_ = null;

  /**
   * Subclasses can invoke the default constructor and then assign the input
   * streams in the open method.
   */
  protected TIOStreamTransport() {}

  /**
   * Input stream constructor, constructs an input only transport.
   *
   * @param is Input stream to read from
   */
  public TIOStreamTransport(InputStream is) {
    inputStream_ = is;
  }

  /**
   * Output stream constructor, constructs an output only transport.
   *
   * @param os Output stream to write to
   */
  public TIOStreamTransport(OutputStream os) {
    outputStream_ = os;
  }

  /**
   * Two-way stream constructor.
   *
   * @param is Input stream to read from
   * @param os Output stream to read from
   */
  public TIOStreamTransport(InputStream is, OutputStream os) {
    inputStream_ = is;
    outputStream_ = os;
  }

  /**
   * 输入输出流、有一个部位null，则表示传输层打开了
   *
   * @return false after close is called.
   */
  public boolean isOpen() {
    return inputStream_ != null || outputStream_ != null;
  }

  /**
   * The streams must already be open. This method does nothing.
   */
  public void open() throws TTransportException {}

  /**
   * 关闭输入输出流：调用close、然后赋值为null
   *
   * Closes both the input and output streams.
   */
  public void close() {
    try {
      if (inputStream_ != null) {
        try {
          inputStream_.close();
        } catch (IOException iox) {
          LOGGER.warn("Error closing input stream.", iox);
        }
      }
      if (outputStream_ != null) {
        try {
          outputStream_.close();
        } catch (IOException iox) {
          LOGGER.warn("Error closing output stream.", iox);
        }
      }
    } finally {
      inputStream_ = null;
      outputStream_ = null;
    }
  }


  /**
   * 从输入流中读取数据到buf：从buf的off位置开始写数据。
   *
   * @param buf Array to read into 存放数据的数据
   * @param off Index to start reading at 开始存放数据的偏移量
   * @param len Maximum number of bytes to read 读取数据的最大值-没有这么多、就不读取这么多
   * @return
   * @throws TTransportException
   */
  public int read(byte[] buf, int off, int len) throws TTransportException {
    //输入流未创建、抛异常
    if (inputStream_ == null) {
      throw new TTransportException(TTransportException.NOT_OPEN, "Cannot read from null inputStream");
    }

    //the total number of bytes read into the buffer,
    int bytesRead;
    try {
      bytesRead = inputStream_.read(buf, off, len);
    } catch (IOException iox) {
      throw new TTransportException(TTransportException.UNKNOWN, iox);
    }

    // or -1 if there is no more data because the end of the stream has been reached.
    // -1 表示流中没有数据了
    if (bytesRead < 0) {
      throw new TTransportException(TTransportException.END_OF_FILE, "Socket is closed by peer.");
    }
    return bytesRead;
  }

  /**
   * 从buf指定的位置开始，写最多len长度的数据到输出流中
   *
   * Writes to the underlying output stream if not null.
   */
  public void write(byte[] buf, int off, int len) throws TTransportException {
    if (outputStream_ == null) {
      throw new TTransportException(TTransportException.NOT_OPEN, "Cannot write to null outputStream");
    }
    try {
      outputStream_.write(buf, off, len);
    } catch (IOException iox) {
      throw new TTransportException(TTransportException.UNKNOWN, iox);
    }
  }

  /**
   * 迫使所有缓冲的输出数据被写出到底层输出流中/
   * 刷新输出流缓存区中的数据到目标位置：
   *  1. 如果不flush就close，可能会丢失数据；
   *
   *
   * Flushes the underlying output stream if not null.
   * https://blog.csdn.net/dabing69221/article/details/16996877
   */
  public void flush() throws TTransportException {
    if (outputStream_ == null) {
      throw new TTransportException(TTransportException.NOT_OPEN, "Cannot flush null outputStream");
    }
    try {
      outputStream_.flush();
    } catch (IOException iox) {
      throw new TTransportException(TTransportException.UNKNOWN, iox);
    }
  }
}
