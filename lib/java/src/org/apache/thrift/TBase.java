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

import java.io.Serializable;

/**
 * Generic base interface for generated Thrift objects.
 */
public interface TBase<T extends TBase<T,F>, F extends TFieldIdEnum> extends Comparable<T>,  TSerializable, Serializable {

  //获取id对应的字段枚举
  public F fieldForId(int fieldId);

  //当前字段是否被赋值
  //如果是基本类型，会有专门的字段标识、否则判断相应的字段是否为null
  public boolean isSet(F field);

  //获取字段枚举对应的字段值
  public Object getFieldValue(F field);

  //使用字段枚举定位字段、然后给该字段赋值
  public void setFieldValue(F field, Object value);

  //深度拷贝
  public T deepCopy();

  //清空所有字段的值
  public void clear();
}
