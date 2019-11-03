package chapter1.function

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}

class MyMapFunction extends RichMapFunction[(String, Int), (String, Int)] {
  override def map(in: (String, Int)): (String, Int) = {
    println(in._1 + "," + in._2)
    (in._1, in._2 + 1)
  }
}
