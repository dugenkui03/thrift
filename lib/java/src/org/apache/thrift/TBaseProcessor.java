package org.apache.thrift;

import java.util.Collections;
import java.util.Map;

import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;

/**
 * 每个自定义service(不是方法)中的Processor类都会实现这个接口：
 *
 *
 */
public abstract class TBaseProcessor<I> implements TProcessor {
  //对应一个service
  private final I iface;

  // service中的方法映射<methodName,ProcessFunction>，其内部类Processor在构造函数中初始化了processMap
  private final Map<String,ProcessFunction<I, ? extends TBase>> processMap;

  /**
   * @param iface 自定义的service文件
   * @param processFunctionMap service中<methodName,ProcessFunction> 映射
   */
  protected TBaseProcessor(I iface, Map<String, ProcessFunction<I, ? extends TBase>> processFunctionMap) {
    this.iface = iface;
    this.processMap = processFunctionMap;
  }

  /**
   * 神器：
   *    1. 获取指定名称对应的ProcessFunction；
   *    2. io协议都是二进制协议，iface也可以获取到，seqid？？如果是预热、随便吧？
   *      process(int seqid, TProtocol iprot, TProtocol oprot, I iface)
   */
  public Map<String,ProcessFunction<I, ? extends TBase>> getProcessMapView() {
    return Collections.unmodifiableMap(processMap);
  }

  @Override
  public void process(TProtocol in, TProtocol out) throws TException {
    //开始处理方法：获取了方法的名称、序列号(重点、序列号)
    TMessage msg = in.readMessageBegin();
    //获取方法名称
    ProcessFunction fn = processMap.get(msg.name);

    //fixme 如果在方法中找到方法对象，调用方法对应ProcessFunction对象的process方法
    if (fn != null) {
      //todo seqid 有啥用
      fn.process(msg.seqid, in, out, iface);
    } else {
      //Skips over the next data element from the provided input TProtocol object
      //从提供的输入协议对象中、跳过下一个数据元素？
      TProtocolUtil.skip(in, TType.STRUCT);
      //读取数据结束；fn!=null的时候，fn.process()里边包含 in.readMessageEnd()的逻辑
      in.readMessageEnd();

      TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'");
      //响应结果：产生异常了。
      out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
      //通过输出协议、响应异常信息
      x.write(out);
      out.writeMessageEnd();
      out.getTransport().flush();
    }
  }
}
