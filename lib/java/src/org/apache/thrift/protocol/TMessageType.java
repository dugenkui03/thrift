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

/**
 * fixme thrift协议的消息类型；
 *
 * Message type constants in the Thrift protocol.
 */
public final class TMessageType {
  /**
   * 请求/调用
   * 响应
   * 异常
   * CALL、REPLY、EXCEPTION、ONEWAY
   */
  public static final byte CALL  = 1;
  public static final byte REPLY = 2;
  //请求到服务端有异常信息是、比如ProcessFunction不存在，使用该tag响应。
  public static final byte EXCEPTION = 3;
  public static final byte ONEWAY = 4;
}
