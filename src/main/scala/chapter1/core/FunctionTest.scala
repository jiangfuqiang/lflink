package chapter1.core

import org.apache.flink.api.scala._
import chapter1.function.MyMapFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object FunctionTest {

  def main(args: Array[String]): Unit ={
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream: DataStream[(String, Int)] = env.fromElements(("a",3),("d",4),("c",2),("c",5),("a",5))

    val mapStream: DataStream[(String,Int)] = dataStream.map(new MyMapFunction)
    mapStream.print()

    val keyedStream: KeyedStream[(String, Int), Tuple] = dataStream.keyBy(0)
    val reduceStream: DataStream[(String, Int)] = keyedStream.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
    reduceStream.print()

    env.execute("FunctionTest")
  }

}
