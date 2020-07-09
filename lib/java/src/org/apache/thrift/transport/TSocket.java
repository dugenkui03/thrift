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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

/**
 * fixme TTransport接口的套接字实现
 *
 * Socket implementation of the TTransport interface. To be commented soon!
 *
 */
public class TSocket extends TIOStreamTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSocket.class.getName());

  /**
   * Wrapped Socket object：被包装的套接字
   */
  private Socket socket_;

  /**
   * Remote host：远端host；
   */
  private String host_;

  /**
   * Remote port：远端端口
   */
  private int port_;

  //Socket timeout - read timeout on the socket：socket读超时
  private int socketTimeout_;

  //Connection timeout：链接超时；
  private int connectTimeout_;

  /**
   * Constructor that takes an already created socket.
   *
   * @param socket Already created socket object
   * @throws TTransportException if there is an error setting up the streams
   */
  public TSocket(Socket socket) throws TTransportException {
    socket_ = socket;
    try {
      socket_.setSoLinger(false, 0);
      socket_.setTcpNoDelay(true);
      socket_.setKeepAlive(true);
    } catch (SocketException sx) {
      LOGGER.warn("Could not configure socket.", sx);
    }

    if (isOpen()) {
      try {
        inputStream_ = new BufferedInputStream(socket_.getInputStream());
        outputStream_ = new BufferedOutputStream(socket_.getOutputStream());
      } catch (IOException iox) {
        close();
        throw new TTransportException(TTransportException.NOT_OPEN, iox);
      }
    }
  }

  /**
   * Creates a new unconnected socket that will connect to the given host
   * on the given port.
   *
   * @param host Remote host
   * @param port Remote port
   */
  public TSocket(String host, int port) {
    this(host, port, 0);
  }

  /**
   * Creates a new unconnected socket that will connect to the given host
   * on the given port.
   *
   * @param host    Remote host
   * @param port    Remote port
   * @param timeout Socket timeout and connection timeout
   */
  public TSocket(String host, int port, int timeout) {
    this(host, port, timeout, timeout);
  }

  /**
   * Creates a new unconnected socket that will connect to the given host
   * on the given port, with a specific connection timeout and a
   * specific socket timeout.
   *
   * @param host            Remote host
   * @param port            Remote port
   * @param socketTimeout   Socket timeout
   * @param connectTimeout  Connection timeout
   */
  public TSocket(String host, int port, int socketTimeout, int connectTimeout) {
    host_ = host;
    port_ = port;
    socketTimeout_ = socketTimeout;
    connectTimeout_ = connectTimeout;
    initSocket();
  }

  //初始化socket对象，使用到了读写超时
  private void initSocket() {
    socket_ = new Socket();
    try {
      socket_.setSoLinger(false, 0);
      socket_.setTcpNoDelay(true);
      socket_.setKeepAlive(true);
      socket_.setSoTimeout(socketTimeout_);
    } catch (SocketException sx) {
      LOGGER.error("Could not configure socket.", sx);
    }
  }


  //读写超时和socket链接超时
  public void setTimeout(int timeout) {
    this.setConnectTimeout(timeout);
    this.setSocketTimeout(timeout);
  }

  //连接超时，毫秒
  public void setConnectTimeout(int timeout) {
    connectTimeout_ = timeout;
  }

  //socket读写超时，毫秒
  public void setSocketTimeout(int timeout) {
    socketTimeout_ = timeout;
    try {
      socket_.setSoTimeout(timeout);
    } catch (SocketException sx) {
      LOGGER.warn("Could not set socket timeout.", sx);
    }
  }

  //返回对底层socket的引用
  public Socket getSocket() {
    if (socket_ == null) {
      initSocket();
    }
    return socket_;
  }

  //socket 是否为已经链接
  public boolean isOpen() {
    if (socket_ == null) {
      return false;
    }
    return socket_.isConnected();
  }

  /**
   * Connects the socket, creating a new socket object if necessary.
   */
  //连接socket
  public void open() throws TTransportException {
    //已经链接、抛异常
    if (isOpen()) {
      throw new TTransportException(TTransportException.ALREADY_OPEN, "Socket already connected.");
    }

    //判断主机和端口是否合法
    if (host_ == null || host_.length() == 0) {
      throw new TTransportException(TTransportException.NOT_OPEN, "Cannot open null host.");
    }
    if (port_ <= 0 || port_ > 65535) {
      throw new TTransportException(TTransportException.NOT_OPEN, "Invalid port " + port_);
    }

    //socket、则使用读写超时创建socket对象
    if (socket_ == null) {
      initSocket();
    }

    try {
      //使用指定connectTimeout和目标主机建立链接
      socket_.connect(new InetSocketAddress(host_, port_), connectTimeout_);

      //fixme 获取socket链接的包装后的输入输出流
      inputStream_ = new BufferedInputStream(socket_.getInputStream());
      outputStream_ = new BufferedOutputStream(socket_.getOutputStream());
    } catch (IOException iox) {
      close();
      throw new TTransportException(TTransportException.NOT_OPEN, iox);
    }
  }

  //关闭socket
  public void close() {
    //关闭底层的io流、点进去见自定义的close方法
    super.close();

    // Close the socket
    if (socket_ != null) {
      try {
        socket_.close();
      } catch (IOException iox) {
        LOGGER.warn("Could not close socket.", iox);
      }
      socket_ = null;
    }
  }

}
