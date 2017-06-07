/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

/**
 * Shims for versions of Hadoop up to and including 2.2.x
 */
public class HadoopShims_2_2 implements HadoopShims {

  //以下两个变量默认是false
  final boolean zeroCopy;//org.apache.hadoop.fs.CacheFlag该类存在,则返回true,否则是false
  final boolean fastRead;//在zeroCopy是true的时候,有readWithKnownLength方法存在,则该属性为true

  HadoopShims_2_2() {
    boolean zcr = false;
    try {
      Class.forName("org.apache.hadoop.fs.CacheFlag", false,
        HadoopShims_2_2.class.getClassLoader());
      zcr = true;
    } catch (ClassNotFoundException ce) {
    }
    zeroCopy = zcr;
    boolean fastRead = false;
    if (zcr) {
      for (Method m : Text.class.getMethods()) {
        if ("readWithKnownLength".equals(m.getName())) {
          fastRead = true;
        }
      }
    }
    this.fastRead = fastRead;
  }

  public DirectDecompressor getDirectDecompressor(
      DirectCompressionType codec) {
    return null;
  }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
                                              ) throws IOException {
    if(zeroCopy) {
      return ZeroCopyShims.getZeroCopyReader(in, pool);
    }
    /* not supported */
    return null;
  }

  private static final class BasicTextReaderShim implements TextReaderShim {
    private final InputStream in;

    public BasicTextReaderShim(InputStream in) {
      this.in = in;
    }

      //读取len个字节,将字节的内容写入到txt中
    @Override
    public void read(Text txt, int len) throws IOException {
      int offset = 0;
      byte[] bytes = new byte[len];//用于缓存
      while (len > 0) {//不断的读取字节,直到读取len个长度为止
        int written = in.read(bytes, offset, len);//读取len个字节数组,存储到bytes中
        if (written < 0) {
          throw new EOFException("Can't finish read from " + in + " read "
              + (offset) + " bytes out of " + bytes.length);
        }
        len -= written;
        offset += written;
      }
      txt.set(bytes);//读取的字节内容写入到txt中
    }
  }

  //读取字节,将其内容写入到Text中
  @Override
  public TextReaderShim getTextReaderShim(InputStream in) throws IOException {
    return new BasicTextReaderShim(in);
  }
}
