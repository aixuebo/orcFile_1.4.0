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
    this.output = new RunLengthByteWriter(output);//RunLengthByteWriter相当于进行了一层序列化操作
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
    int bitsToWrite = bitSize;//表示要从value中获取多少个bit
    while (bitsToWrite > bitsLeft) {//说明要从value获取的bit数据不能全部都存储到current中,因为current剩余的位置不够存储
        /**
         * 比如要从value中获取12个bit,即bitSize为12,但是bitsLeft最多也就8个bit,因此要做如下处理:
         * 1.计算有多少个bit是不能容纳的,比如12-8=4,即4个bit不能被容纳,因此先不处理这4个.先处理能容纳的8个
         * 即value >>> (bitsToWrite - bitsLeft); 表示value无符号的右移4个位置,即删除后面4个,前面用0补位,即假设原来是1101 1111 0011 现在变成0000 1101 1111,这样相当于只是获取了value原始的前面8个bit
         * 2.减少从value尚未获取的bit数,即value从12bit变成4bit,即还有4bit没有被写入
         * 3.为value更改内容,
         * 即value已经从1101 1111 0011 因为还剩下4bit,因此变成1111,然后与value进行&操作,剩余的就是value的最后4个bit
         */
      //注意:value >>> (bitsToWrite - bitsLeft); 操作虽然将value做了改变,但是改变后的值,没有赋值给value,因此value还是原来的value
      // add the bits to the bottom of the current word
      current |= value >>> (bitsToWrite - bitsLeft);//在步骤1操作,然后将剩余的数据与current做|处理
      // subtract out the bits we just added
      bitsToWrite -= bitsLeft;//减少从value尚未获取的bit数
      // zero out the bits above bitsToWrite
      //1 << bitsToWrite 表示1后面追加bitsToWrite个0,比如bitsToWrite=3,则表示1000,然后-1,得到的结果就是bitsToWrite个1,即111
      //该算法就是value & bitsToWrite个1进行与操作,即只是保留value的bitsToWrite个bit有意义,其他都丢弃
      value &= (1 << bitsToWrite) - 1;//即此时value只保留了原始value的最后bitsToWrite位置----具体参见步骤3逻辑
      writeByte();
    }
      //说明bitsLeft>bitsToWrite,即有多余的位置容纳bitsToWrite,bitsToWrite表示value需要的字节数
      //此时说明有足够的字节空间去容纳bitsToWrite
    bitsLeft -= bitsToWrite;//减去bitsToWrite个字节
    //原来bitsLeft=8,因为此时加入了一个value占用一个字节,因此bitsLeft剩余位置就是7,因此value << bitsLeft 就将value多添加7个0,比如value是1011 1001  现在就变成1000 0000
    //而current=0初始化,因此他的所有8个bit位置都是0,参与|运算的时候,只要是1,他结果就是1,因此此时为current的最左边的一个bit赋值,因此0000 0000 | 1000 0000,因此结果刚刚取消的那个位置就被赋予value该位置的值了

    /*
     * 这块比较难理解,那么在举例一下
     * 比如bitsToWrite是3,表示一次要写入3个bit,即value只有3个bit有用,
     * 而bitsLeft还剩余6个,因此current此时的值是xx00 0000,即只有前两个bit有值
     * 然后将value的3个bit写入到current中,反推的话,因为value只有3个bit,而current此时的值是xx00 0000,因此value应该变成00xx x000,这样就能把本次value的内容写入到current中了。
     * 为了达到该目的,我们将value后面追加3个0,即先将bitsLeft从6变成6-3=3,然后追加bitsLeft个0.因此value就从xxx变成了00xx x000了
     *
     *
     * 注意:value << bitsLeft操作虽然将value做了改变,但是改变后的值,没有赋值给value,因此value还是原来的value
     */
    current |= value << bitsLeft;//value追加bitsLeft个0,比如value是1011 1001,bitsLeft为3,即还能保存3个bit到current中,因此1011 1001变成1 1001 000
    if (bitsLeft == 0) {
      writeByte();
    }
  }

    //记录当时文件的position以及此时已经写入了多少个bit在current中
  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(8 - bitsLeft);
  }

  public long estimateMemory() {
    return output.estimateMemory();
  }
}
