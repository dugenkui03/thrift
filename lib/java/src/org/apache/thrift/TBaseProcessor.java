package org.apache.thrift;

import java.util.Collections;
import java.util.Map;

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;

/**
 *
 * 对应一个接口类、即一个service定义。
 *
 * 每个方法都是TBaseProcessor子类的内部类，其内部类继承了ProcessFunction：
 *
 * getProcessMap 方法包含了所有方法的映射<methodName,ProcessFunction>
 */
public abstract class TBaseProcessor<I> implements TProcessor {
  private final I iface;
  private final Map<String,ProcessFunction<I, ? extends TBase>> processMap;

  protected TBaseProcessor(I iface, Map<String, ProcessFunction<I, ? extends TBase>> processFunctionMap) {
    this.iface = iface;
    this.processMap = processFunctionMap;
  }

  public Map<String,ProcessFunction<I, ? extends TBase>> getProcessMapView() {
    return Collections.unmodifiableMap(processMap);
  }

  @Override
  public void process(TProtocol in, TProtocol out) throws TException {
    //输入协议：开始读取数据
    TMessage msg = in.readMessageBegin();
    //获取方法名称
    ProcessFunction fn = processMap.get(msg.name);

    //fixme 如果在方法中找到方法对象，调用方法对应ProcessFunction对象的process方法
    if (fn != null) {
      fn.process(msg.seqid, in, out, iface);
    } else {
      //Skips over the next data element from the provided input TProtocol object
      //从提供的输入协议对象中、跳过下一个数据元素？
      TProtocolUtil.skip(in, TType.STRUCT);
      //读取数据结束
      in.readMessageEnd();

      TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'");
      out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
      x.write(out);
      out.writeMessageEnd();
      out.getTransport().flush();
    }
  }
}
