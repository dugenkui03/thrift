package org.apache.thrift;

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义service的方法、与其内部类Processor中的每个内部方法类一一对应，这些类方法继承了ProcessFunction
 * @param <I>
 * @param <T> 对方法
 */
public abstract class ProcessFunction<I, T extends TBase> {

  //方法名称
  private final String methodName;

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessFunction.class.getName());

  public ProcessFunction(String methodName) {
    this.methodName = methodName;
  }

  /**
   * @param seqid 序列id
   * @param iprot 输入协议
   * @param oprot 输出协议
   * @param iface 对应一个自定义的service，里边包含methodName
   */
  public final void process(int seqid, TProtocol iprot, TProtocol oprot, I iface) throws TException {
    //获取该方法参数包装类、但是没有set任何数据
    T args = getEmptyArgsInstance();
    try {
      //todo 数据读取到哪了？读取到args中了？、哇～～～～
      args.read(iprot);
    } catch (TProtocolException e) {
      iprot.readMessageEnd();
      TApplicationException applicationException = new TApplicationException(TApplicationException.PROTOCOL_ERROR, e.getMessage());
      oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
      applicationException.write(oprot);
      oprot.writeMessageEnd();
      oprot.getTransport().flush();
      return;
    }
    iprot.readMessageEnd();


    byte msgType = TMessageType.REPLY;
    TSerializable result = null;
    try {
      /**fixme 注意、继承该抽象类的类就是Processor中的内部方法类，所以getResult肯定知道调用哪个方法、生成代码写死的。demo
       *       protected query_result getResult(I iface, query_args args) throws TException {
       *         query_result result = new query_result();
       *         result.success = iface.query(args.param);
       *         return result;
       *       }
       */
      result = getResult(iface, args);
    } catch (TTransportException ex) {
      LOGGER.error("Transport error while processing " + getMethodName(), ex);
      throw ex;
    } catch (TApplicationException ex) {
      LOGGER.error("Internal application error processing " + getMethodName(), ex);
      result = ex;
      msgType = TMessageType.EXCEPTION;
    } catch (Exception ex) {
      LOGGER.error("Internal error processing " + getMethodName(), ex);
      if(rethrowUnhandledExceptions()) throw new RuntimeException(ex.getMessage(), ex);
      if(!isOneway()) {
        result = new TApplicationException(TApplicationException.INTERNAL_ERROR,
            "Internal error processing " + getMethodName());
        msgType = TMessageType.EXCEPTION;
      }
    }

    if(!isOneway()) {
      oprot.writeMessageBegin(new TMessage(getMethodName(), msgType, seqid));
      result.write(oprot);
      oprot.writeMessageEnd();
      oprot.getTransport().flush();
    }
  }

  private void handleException(int seqid, TProtocol oprot) throws TException {
    if (!isOneway()) {
      TApplicationException x = new TApplicationException(TApplicationException.INTERNAL_ERROR,
        "Internal error processing " + getMethodName());
      oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
      x.write(oprot);
      oprot.writeMessageEnd();
      oprot.getTransport().flush();
    }
  }

  protected boolean rethrowUnhandledExceptions(){
    return false;
  }

  protected abstract boolean isOneway();

  /**
   * 每个接口都实现了
   * @param iface
   * @param args
   * @return
   * @throws TException
   */
  public abstract TBase getResult(I iface, T args) throws TException;

  /**
   * 获取方法参数包装类、但是没有set任何数据
   *       protected 方法名称_args getEmptyArgsInstance() {
   *         return new 方法名称_args();
   *       }
   */
  public abstract T getEmptyArgsInstance();

  public String getMethodName() {
    return methodName;
  }
}
