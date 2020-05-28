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
 * 封装结构元数据的协助类
 *
 * Helper class that encapsulates struct metadata.
 */
public final class TStruct {

  //类名称
  public final String name;

  //每个自定义的TBase实现类都有一个TStruct属性，其构造参数变量为类名称
  public TStruct(String n) {
    name = n;
  }

  public TStruct() {
    this("");
  }
}
