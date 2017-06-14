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

import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.RunLengthByteWriter;

import java.io.IOException;

public class BitFieldWriter {
  private RunLengthByteWriter output;
  private final int bitSize;//表示一次value占用几个bit,虽然传入的value是一个int值,但是其实他不一定需要32个bit
  private byte current = 0;//最终8个字节的内容
  private int bitsLeft = 8;//凑够8个字节就写入一次

    /**
     *
     * @param output
     * @param bitSize   一般传入的时候都是1,表示该value占用几个bit
     * @throws IOException
     */
  public BitFieldWriter(PositionedOutputStream output,
                 int bitSize) throws IOException {
    this.output = new RunLengthByteWriter(output);
    this.bitSize = bitSize;
  }

  private void writeByte() throws IOException {
    output.write(current);//将current写入到输出中
    current = 0;
    bitsLeft = 8;
  }

  public void flush() throws IOException {
    if (bitsLeft != 8) {//说明有数据,因此要写入
      writeByte();
    }
    output.flush();
  }

    //添加一个value
  public void write(int value) throws IOException {
    int bitsToWrite = bitSize;
    while (bitsToWrite > bitsLeft) {//false,说明bitsLeft>bitsToWrite,即有多余的位置容纳bitsToWrite,bitsToWrite表示value需要的字节数
      // add the bits to the bottom of the current word
      current |= value >>> (bitsToWrite - bitsLeft);
      // subtract out the bits we just added
      bitsToWrite -= bitsLeft;
      // zero out the bits above bitsToWrite
      value &= (1 << bitsToWrite) - 1;
      writeByte();
    }
      //此时说明有足够的字节空间去容纳bitsToWrite
    bitsLeft -= bitsToWrite;//减去bitsToWrite个字节
    current |= value << bitsLeft;//value追加bitsLeft个0
    if (bitsLeft == 0) {
      writeByte();
    }
  }

  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(8 - bitsLeft);
  }

  public long estimateMemory() {
    return output.estimateMemory();
  }
}
