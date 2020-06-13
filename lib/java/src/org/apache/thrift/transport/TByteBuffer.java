package org.apache.thrift.transport;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * nio的ByteBuffer支持的传输实现
 *
 * ByteBuffer-backed implementation of TTransport.
 */
public final class TByteBuffer extends TTransport {
  private final ByteBuffer byteBuffer;

  /**
   * 使用指定的 ByteBuffer 对象创建 TByteBuffer对象：组合。
   *
   * Creates a new TByteBuffer wrapping a given NIO ByteBuffer.
   */
  public TByteBuffer(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public void open() {
  }

  @Override
  public void close() {
  }

  //将对象包装的数据，从off开始、拷贝len长度的数据到buf缓冲区
  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    //byteBuffer.remaining()=limit - position：byteBuffer中剩余的可读取的元素数量
    // 计算可读取 byteBuffer 的字节数量
    final int n = Math.min(byteBuffer.remaining(), len);
    if (n > 0) {
      try {
        //fixme 是buf的偏移量
        byteBuffer.get(buf, off, n);
      } catch (BufferUnderflowException e) {
        throw new TTransportException("Unexpected end of input buffer", e);
      }
    }
    return n;
  }

  //fixme 读取数据到byteBuffer中
  //buf 要从中读取字节的数组
  //off 开始读取buf的偏移量
  //len 要读取字节的最大长度
  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    try {
      //将buf中指定起始位置、长度的数据写入到byteBuffer中
      byteBuffer.put(buf, off, len);
    } catch (BufferOverflowException e) {
      throw new TTransportException("Not enough room in output buffer", e);
    }
  }

  /**
   * 回去该对象包装的 缓冲区
   *
   * Get the underlying NIO ByteBuffer.
   */
  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  /**
   * Buffer中的数据没有清空，如果通过Buffer.get(i)的方式还是可以访问到数据的。如果再次向缓冲区中写入数据、会覆盖之前存在的数据
   *
   * Convenience method to call clear() on the underlying NIO ByteBuffer.、
   *
   * <pre>
   * {@code
   *     public final Buffer clear() {
   *         position = 0;
   *         limit = capacity;
   *         mark = -1;
   *         return this;
   *     }
   * }
   * </pre>
   */
  public TByteBuffer clear() {
    byteBuffer.clear();
    return this;
  }

  /**
   * 切换到读模式，指针指向起始位置，其他如下
   *
   * <pre>
   *     {@code
   *         public final Buffer flip() {
   *         limit = position;
   *         position = 0;
   *         mark = -1;
   *         return this;
   *     }
   *     }
   * </pre>
   *
   * Convenience method to call flip() on the underlying NIO ByteBuffer.
     */
  public TByteBuffer flip() {
    byteBuffer.flip();
    return this;
  }

  /**
   * 将ByteBuffer转为普通字节数组->只考虑待读取的数据
   *
   * Convenience method to convert the underlying NIO ByteBuffer to a
   * plain old byte array.
   */
  public byte[] toByteArray() {
    //byteBuffer中剩余的可读取的元素数量
    final byte[] data = new byte[byteBuffer.remaining()];
    //https://www.jianshu.com/p/451cc865d413
    //创建一个分片缓冲区。分配缓冲区与主缓冲区共享数据。
    //分配的起始位置是主缓冲区的position位置
    //容量为limit-position。
    //分片缓冲区无法看到主缓冲区positoin之前的元素。
    byteBuffer.slice().get(data);
    return data;
  }
}
