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
import java.nio.charset.Charset;

/**
 * fixme： 比父类ByteArrayOutputStream多了三个语法糖方法：获取byte数组 buf，buf的大小、清空buf(buf = new byte[initialSize])
 */
public class TByteArrayOutputStream extends ByteArrayOutputStream {

  private final int initialSize;

  public TByteArrayOutputStream(int size) {
    super(size);
    this.initialSize = size;
  }

  public TByteArrayOutputStream() {
    this(32);
  }

  //fixme 获取存储数据的缓存区
  public byte[] get() {
    return buf;
  }

  //重置为空byte数组
  public void reset() {
    // count = 0: The number of valid bytes in the buffer.
    super.reset();
    if (buf.length > initialSize) {
      //初始化缓存区
      buf = new byte[initialSize];
    }
  }

  //获取缓存区大小
  public int len() {
    return count;
  }

  public String toString(Charset charset) {
    return new String(buf, 0, count, charset);
  }
}
