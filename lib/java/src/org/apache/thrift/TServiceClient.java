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

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

/**
 * A TServiceClient is used to communicate with a TService implementation across protocols and transports.
 *
 * TServiceClient通过 协议 和 传输层 与目标Service进行互动。
 */
public abstract class TServiceClient {
  public TServiceClient(TProtocol prot) {
    this(prot, prot);
  }

  public TServiceClient(TProtocol iprot, TProtocol oprot) {
    iprot_ = iprot;
    oprot_ = oprot;
  }

  //input协议、output协议
  protected TProtocol iprot_;
  protected TProtocol oprot_;

  //序列ID?
  protected int seqid_;

  /**
   * Get the TProtocol being used as the input (read) protocol.
   * @return the TProtocol being used as the input (read) protocol.
   */
  public TProtocol getInputProtocol() {
    return this.iprot_;
  }

  /**
   * Get the TProtocol being used as the output (write) protocol.
   * @return the TProtocol being used as the output (write) protocol.
   */
  public TProtocol getOutputProtocol() {
    return this.oprot_;
  }

  /**
   * TServiceClient通过 协议 和 传输层 与目标Service进行互动。
   *
   * @param methodName  接口名称
   * @param args  参数包装类、参数数据信息
   */
  protected void sendBase(String methodName, TBase<?,?> args) throws TException {
    sendBase(methodName, args, TMessageType.CALL);
  }

  protected void sendBaseOneway(String methodName, TBase<?,?> args) throws TException {
    sendBase(methodName, args, TMessageType.ONEWAY);
  }

  /**
   * @param methodName  接口名称
   * @param args 参数的包装类，也是TBase的实现类
   * @param type 消息类型：请求、响应、异常等，见TMessageType.CALL
   */
  private void sendBase(String methodName, TBase<?,?> args, byte type) throws TException {

    //包装方法名称和调用序号
    TMessage tMessage = new TMessage(methodName, type, ++seqid_);

    /**开始写消息：方法名称、类型、调用序列id；方法参数
     */

    //将方法名称、类型和序列id 写入到目标buf/流中，在向远端传递参数数据之前调用
    oprot_.writeMessageBegin(tMessage);
    //写入数据到指定协议，就是TBase类中的write方法，这里是方法对应的TBase类。write方法内容示例如下
    //
    //    public void write(TProtocol oprot) throws TException {
    //      //获取指定类型schema的创建工厂
    //      SchemeFactory schemeFactory = schemes.get(oprot.getScheme());
    //      //创建schema
    //      IScheme scheme = schemeFactory.getScheme();
    //      //schema使用指定的协议写此参数对象
    //      //fixme schema参见 methodName_argsStandardScheme 和 methodName_argsTupleScheme
    //      scheme.write(oprot, this);//见下
    //    }
    //      //写一个流中写数据、发送请求的时候使用
    //      public void write(TProtocol oprot, methodName_args struct) throws TException {
    //        struct.validate();
    //
    //        //STRUCT_DESC的name是参数包装类名称，即methodName_args
    //        oprot.writeStructBegin(STRUCT_DESC);
    //
    //        //如果包装类methodName_args中的参数不为空
    //        if (struct.param != null) {
    //          //开始写参数字段
    //          oprot.writeFieldBegin(PARAM_FIELD_DESC);
    //          //见参数类中的中的XXParamStandardScheme
    //          struct.param.write(oprot);
    //          oprot.writeFieldEnd();
    //        }
    //        // todo 为什么不在if判断里边
    //        oprot.writeFieldStop();
    //
    //        oprot.writeStructEnd();
    //      }
    args.write(oprot_);

    //结束写消息
    oprot_.writeMessageEnd();
    oprot_.getTransport().flush();//flush: 清空
  }

  /**
   * 接受请求结果
   *
   * @param result 对结果的包装、唯一属性就是结果对象
   * @param methodName 请求方法名称
   */
  protected void receiveBase(TBase<?,?> result, String methodName) throws TException {
    TMessage msg = iprot_.readMessageBegin();
    if (msg.type == TMessageType.EXCEPTION) {
      TApplicationException x = new TApplicationException();
      x.read(iprot_);
      iprot_.readMessageEnd();
      throw x;
    }
    if (msg.seqid != seqid_) {
      throw new TApplicationException(TApplicationException.BAD_SEQUENCE_ID,
          String.format("%s failed: out of sequence response: expected %d but got %d", methodName, seqid_, msg.seqid));
    }
    result.read(iprot_);
    iprot_.readMessageEnd();
  }
}
