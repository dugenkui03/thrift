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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 异步客户端的传输层
 * Transport for use with async client.
 */
public class TNonblockingSocket extends TNonblockingTransport {

  /**
   * 主机和端口，socketChannel_连接的目标主机
   * Host and port if passed in, used for lazy non-blocking connect.
   */
  private final SocketAddress socketAddress_;

  /** http://ifeve.com/socket-channel/
   * Java NIO中的SocketChannel是一个连接到TCP(三次握手)网络套接字的通道。可以通过以下2种方式创建SocketChannel：
   *
   *  1. 打开一个SocketChannel并连接到互联网上的某台服务器；
   *  2. 一个新连接到达ServerSocketChannel时，会创建一个SocketChannel。
   */
  private final SocketChannel socketChannel_;

  //目标主机和端口
  public TNonblockingSocket(String host, int port) throws IOException {
    this(host, port, 0);
  }

  /**
   * 使用指定的超时和客户端 主机:port，创建传输层
   */
  public TNonblockingSocket(String host, int port, int timeout) throws IOException {
    //open()：打开并返回一个新的通道；打开超时时间；网络套接字地址
    this(SocketChannel.open(), timeout, new InetSocketAddress(host, port));
  }

  //使用一个已经存在的套接字、创建传输层。
  public TNonblockingSocket(SocketChannel socketChannel) throws IOException {
    this(socketChannel, 0, null);
    //如果创建失败
    if (!socketChannel.isConnected()) {
      throw new IOException("Socket must already be connected");
    }
  }

  /**
   *
   * @param socketChannel 套接字通道
   * @param timeout
   * @param socketAddress
   * @throws IOException
   */
  private TNonblockingSocket(SocketChannel socketChannel, int timeout, SocketAddress socketAddress)
      throws IOException {
    //对象成员变量指向构造函数参数
    socketChannel_ = socketChannel;
    socketAddress_ = socketAddress;

    // make it a nonblocking channel
    // 使用非阻塞套接字通道，异步模式下调用connect(), read() 和write()
    // 例如，此时调用connect()、该方法可能在连接建立之前就返回了
    socketChannel.configureBlocking(false);

    // set options
    Socket socket = socketChannel.socket();
    socket.setSoLinger(false, 0);
    socket.setTcpNoDelay(true);
    socket.setKeepAlive(true);
    setTimeout(timeout);
  }

  /**
   * 使用指定的Selector注册一个新socket通道，表示准备好IO了
   * Register the new SocketChannel with our Selector,
   * indicating we'd like to be notified when it's ready for I/O.
   *
   * @param selector http://ifeve.com/selectors/
   *                 Selector（选择器）是Java NIO中能够检测一到多个NIO通道，并能够知晓通道是否为诸如读写事件做好准备的组件。
   *                 这样，一个单独的线程可以管理多个channel，从而管理多个网络连接。
   *
   * @return the selection key for this socket. 套接字的key
   */
  public SelectionKey registerSelector(Selector selector, int interests) throws IOException {
    return socketChannel_.register(selector, interests);
  }

  /**
   * 从套接字读写超时：fixme 平时所说的超时、都是套接字读写超时(连接或者读写)
   *
   * Sets the socket timeout, although this implementation never uses blocking operations so it is unused.
   * @param timeout Milliseconds timeout
   */
  public void setTimeout(int timeout) {
    try {
      socketChannel_.socket().setSoTimeout(timeout);
    } catch (SocketException sx) {
      LOGGER.warn("Could not set socket timeout.", sx);
    }
  }

  /**
   * Returns a reference to the underlying SocketChannel.
   */
  public SocketChannel getSocketChannel() {
    return socketChannel_;
  }

  /**
   * Checks whether the socket is connected.
   */
  public boolean isOpen() {
    // isConnected() does not return false after close(), but isOpen() does
    return socketChannel_.isOpen() && socketChannel_.isConnected();
  }

  /**
   * Do not call, the implementation provides its own lazy non-blocking connect.
   */
  public void open() throws TTransportException {
    throw new RuntimeException("open() is not implemented for TNonblockingSocket");
  }

  //fixme
  //    从目标主机读取数据到这个buffer中
  //    buf[] <- data - webServer
  //    返回值代表读取的值的字节数，如果返回-1则代表读取了流的末尾或者socket关闭了连接
  public int read(ByteBuffer buffer) throws IOException {
    return socketChannel_.read(buffer);
  }


  /**
   * Reads from the underlying input stream if not null.
   */
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if ((socketChannel_.validOps() & SelectionKey.OP_READ) != SelectionKey.OP_READ) {
      throw new TTransportException(TTransportException.NOT_OPEN,
        "Cannot read from write-only socket channel");
    }
    try {
      return socketChannel_.read(ByteBuffer.wrap(buf, off, len));
    } catch (IOException iox) {
      throw new TTransportException(TTransportException.UNKNOWN, iox);
    }
  }

  //Perform a nonblocking write of the data in buffer;
  //将缓存区中的数据写入到channel、发送给远端的主机
  public int write(ByteBuffer buffer) throws IOException {
    return socketChannel_.write(buffer);
  }

  //将buf中、off开始的长度为len的数据，写入到对象 socket channel 中
  public void write(byte[] buf, int off, int len) throws TTransportException {
    //判断是否允许想socket写数据，不允许的话返回异常
    if ((socketChannel_.validOps() & SelectionKey.OP_WRITE) != SelectionKey.OP_WRITE) {
      throw new TTransportException(TTransportException.NOT_OPEN,
        "Cannot write to write-only socket channel");
    }
    try {
      socketChannel_.write(ByteBuffer.wrap(buf, off, len));
    } catch (IOException iox) {
      throw new TTransportException(TTransportException.UNKNOWN, iox);
    }
  }

  /**
   * Noop.
   */
  public void flush() throws TTransportException {
    // Not supported by SocketChannel.
  }

  /**
   * Closes the socket.
   */
  public void close() {
    try {
      socketChannel_.close();
    } catch (IOException iox) {
      LOGGER.warn("Could not close socket.", iox);
    }
  }

  //创建一个套接字通道、并返回是否创建成功
  public boolean startConnect() throws IOException {
    return socketChannel_.connect(socketAddress_);
  }

  /** {@inheritDoc} */
  public boolean finishConnect() throws IOException {
    return socketChannel_.finishConnect();
  }

  @Override
  public String toString() {
    return "[remote: " + socketChannel_.socket().getRemoteSocketAddress() +
        ", local: " + socketChannel_.socket().getLocalAddress() + "]" ;
  }
}
