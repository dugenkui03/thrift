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

package org.apache.thrift.transport.sasl;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * 帧头部定义。
 * <p></p>
 * 数组帧头部，对于每一帧、头部包含数据大小和其他元数据。
 * Read headers for a frame. For each frame, the header contains payload size and other metadata.
 */
public interface FrameHeaderReader {

  /**
   * 作为 thrift简单验证安全层 规范所诉，所有的安全数据(谈判和发送数据)应该有header来表明 净负荷。
   * As the thrift sasl specification states, all sasl messages (both for negotiatiing and for
   * sending data) should have a header to indicate the size of the payload.
   *
   * @return size of the payload. 数据帧净负荷的大小-存数据。
   */
  int payloadSize();

  /**
   * 为头部接收的数据。
   *
   * @return The received bytes for the header.
   * @throws IllegalStateException if isComplete returns false.
   */
  byte[] toBytes();

  /**
   * 如果header中所有的字段都已经设置、则为true.
   *
   * @return true if this header has all its fields set.
   */
  boolean isComplete();

  /**
   * 清除header、并且使其能够读取新的头部。
   * Clear the header and make it available to read a new header.
   */
  void clear();

  /**
   * (Nonblocking) Read fields from underlying transport layer.
   *
   * @param transport underlying transport.
   * @return true if header is complete after read.
   * @throws TSaslNegotiationException if fail to read a valid header of a sasl negotiation message.
   * @throws TTransportException if io error.
   */
  boolean read(TTransport transport) throws TSaslNegotiationException, TTransportException;
}
