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
package org.apache.thrift.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * fixme
 *    成员变量是方法的参数；
 *    write_args()
 *
 * Encapsulates(压缩) an async method call.
 * <p>
 * Need to generate:
 * <ul>
 *   <li>protected abstract void write_args(TProtocol protocol)</li>
 *   <li>protected abstract T getResult() throws &lt;Exception_1&gt;, &lt;Exception_2&gt;, ...</li>
 * </ul>
 *
 * @param <T> The return type of the encapsulated method call.
 */
public abstract class TAsyncMethodCall<T> {

  //初始化内存大小，静态的、对所有的对象都适用
  private static final int INITIAL_MEMORY_BUFFER_SIZE = 128;

  //fixme 序列ID，idl中定义的每个service方法都对应一个序列id、在初始化该方法类的构造函数中被调用
  private static AtomicLong sequenceIdCounter = new AtomicLong(0);

  //状态
  public static enum State {
    CONNECTING,//连接的
    WRITING_REQUEST_SIZE,//写请求大小
    WRITING_REQUEST_BODY,//写请求体
    READING_RESPONSE_SIZE,//读取响应大小
    READING_RESPONSE_BODY,//读响应体
    RESPONSE_READ,//响应读取
    ERROR;//错误
  }

  // Next step in the call, initialized by start()
  // 这个调用下一步进行的操作、在start()中初始化
  private State state = null;

  protected final TNonblockingTransport transport;
  private final TProtocolFactory protocolFactory;
  protected final TAsyncClient client;
  //异步方法会回调
  private final AsyncMethodCallback<T> callback;
  private final boolean isOneway;
  private long sequenceId;
  private final long timeout;

  //保存帧数据
  private ByteBuffer sizeBuffer;
  private final byte[] sizeBufferArray = new byte[4];
  private ByteBuffer frameBuffer;

  private long startTime = System.currentTimeMillis();

  protected TAsyncMethodCall(TAsyncClient client, TProtocolFactory protocolFactory, TNonblockingTransport transport, AsyncMethodCallback<T> callback, boolean isOneway) {
    this.transport = transport;
    this.callback = callback;
    this.protocolFactory = protocolFactory;
    this.client = client;
    this.isOneway = isOneway;
    this.sequenceId = TAsyncMethodCall.sequenceIdCounter.getAndIncrement();
    this.timeout = client.getTimeout();
  }

  protected State getState() {
    return state;
  }

  protected boolean isFinished() {
    return state == State.RESPONSE_READ;
  }

  protected long getStartTime() {
    return startTime;
  }

  protected long getSequenceId() {
    return sequenceId;
  }

  public TAsyncClient getClient() {
    return client;
  }

  public boolean hasTimeout() {
    return timeout > 0;
  }

  public long getTimeoutTimestamp() {
    return timeout + startTime;
  }

  //将方法参数写想远端服务
  protected abstract void write_args(TProtocol protocol) throws TException;

  protected abstract T getResult() throws Exception;

  /**
   * 方法调用前置方法
   * @throws TException if buffer initialization fails
   */
  protected void prepareMethodCall() throws TException {

    //创建 对一个流的读写缓存
    TMemoryBuffer memoryBuffer = new TMemoryBuffer(INITIAL_MEMORY_BUFFER_SIZE);

    //使用内存传输类型(TMemoryBuffer extends TTransport)、创建一个协议工厂
    TProtocol protocol = protocolFactory.getProtocol(memoryBuffer);

    //将方法参数写想远端服务
    write_args(protocol);

    //todo?
    int length = memoryBuffer.length();

    //将memoryBuffer中的数据包装成ByteBuffer类
    frameBuffer = ByteBuffer.wrap(memoryBuffer.getArray(), 0, length);

    //sizeBufferArray byte[4]数组
    TFramedTransport.encodeFrameSize(length, sizeBufferArray);
    sizeBuffer = ByteBuffer.wrap(sizeBufferArray);
  }

  /**
   * Register with selector and start first state, which could be either connecting or writing.
   * @throws IOException if register or starting fails
   *
   * Selector（选择器）是Java NIO中能够检测一到多个NIO通道，
   * 并能够知晓通道是否为诸如读写事件做好准备的组件。
   * 这样，一个单独的线程可以管理多个channel，从而管理多个网络连接。
   */
  void start(Selector sel) throws IOException {
    SelectionKey key;

    //如果传输层打开了
    if (transport.isOpen()) {
      //写请求任务大小？
      state = State.WRITING_REQUEST_SIZE;
      //Registers this channel with the given selector, returning a selection key.
      //使用指定的selector和key注册transport，并返回key
      key = transport.registerSelector(sel, SelectionKey.OP_WRITE);
    } else {
      state = State.CONNECTING;
      key = transport.registerSelector(sel, SelectionKey.OP_CONNECT);

      // non-blocking connect can complete immediately,
      // in which case we should not expect the OP_CONNECT
      //非阻塞连接可以立即完成，这种情况下我们不能够期望 OP_CONNECT
      //startConnect操作含义：例如、连接一个socket通道、并返回是否成功
      if (transport.startConnect()) {

        registerForFirstWrite(key);
      }
    }

    key.attach(this);
  }

  //为第一次写、进行注册
  void registerForFirstWrite(SelectionKey key) throws IOException {
    //获取写请求大小
    state = State.WRITING_REQUEST_SIZE;
    //将SelectionKey设置为给定的值
    key.interestOps(SelectionKey.OP_WRITE);
  }

  protected ByteBuffer getFrameBuffer() {
    return frameBuffer;
  }

  /**
   * Transition to next state, doing whatever work is required. Since this
   * method is only called by the selector thread, we can make changes to our
   * select interests without worrying about concurrency.
   * @param key
   */
  void transition(SelectionKey key) {
    // Ensure key is valid
    if (!key.isValid()) {
      key.cancel();
      Exception e = new TTransportException("Selection key not valid!");
      onError(e);
      return;
    }

    // Transition function
    try {
      switch (state) {
        case CONNECTING:
          doConnecting(key);
          break;
        case WRITING_REQUEST_SIZE:
          doWritingRequestSize();
          break;
        case WRITING_REQUEST_BODY:
          doWritingRequestBody(key);
          break;
        case READING_RESPONSE_SIZE:
          doReadingResponseSize();
          break;
        case READING_RESPONSE_BODY:
          doReadingResponseBody(key);
          break;
        default: // RESPONSE_READ, ERROR, or bug
          throw new IllegalStateException("Method call in state " + state
              + " but selector called transition method. Seems like a bug...");
      }
    } catch (Exception e) {
      key.cancel();
      key.attach(null);
      onError(e);
    }
  }

  protected void onError(Exception e) {
    client.onError(e);
    callback.onError(e);
    state = State.ERROR;
  }

  private void doReadingResponseBody(SelectionKey key) throws IOException {
    if (transport.read(frameBuffer) < 0) {
      throw new IOException("Read call frame failed");
    }
    if (frameBuffer.remaining() == 0) {
      cleanUpAndFireCallback(key);
    }
  }

  private void cleanUpAndFireCallback(SelectionKey key) {
    state = State.RESPONSE_READ;
    key.interestOps(0);
    // this ensures that the TAsyncMethod instance doesn't hang around
    key.attach(null);
    try {
      T result = this.getResult();
      client.onComplete();
      callback.onComplete(result);
    } catch (Exception e) {
      key.cancel();
      onError(e);
    }
  }

  private void doReadingResponseSize() throws IOException {
    if (transport.read(sizeBuffer) < 0) {
      throw new IOException("Read call frame size failed");
    }
    if (sizeBuffer.remaining() == 0) {
      state = State.READING_RESPONSE_BODY;
      //获取帧大小、并分配响应的内存块
      int frameSize = TFramedTransport.decodeFrameSize(sizeBufferArray);
      frameBuffer = ByteBuffer.allocate(frameSize);
    }
  }

  private void doWritingRequestBody(SelectionKey key) throws IOException {
    if (transport.write(frameBuffer) < 0) {
      throw new IOException("Write call frame failed");
    }
    if (frameBuffer.remaining() == 0) {
      if (isOneway) {
        cleanUpAndFireCallback(key);
      } else {
        state = State.READING_RESPONSE_SIZE;
        sizeBuffer.rewind();  // Prepare to read incoming frame size
        key.interestOps(SelectionKey.OP_READ);
      }
    }
  }

  private void doWritingRequestSize() throws IOException {
    if (transport.write(sizeBuffer) < 0) {
      throw new IOException("Write call frame size failed");
    }
    if (sizeBuffer.remaining() == 0) {
      state = State.WRITING_REQUEST_BODY;
    }
  }

  private void doConnecting(SelectionKey key) throws IOException {
    if (!key.isConnectable() || !transport.finishConnect()) {
      throw new IOException("not connectable or finishConnect returned false after we got an OP_CONNECT");
    }
    registerForFirstWrite(key);
  }
}
