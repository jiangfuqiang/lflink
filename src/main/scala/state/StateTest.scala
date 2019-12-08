package state

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object StateTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    val inputData: DataStream[String] = env.socketTextStream("localhost", 9090)
    val resultData = inputData.filter(_.nonEmpty).map(new MapFunction[String, (Long, String, Int)](

    ) {
      override def map(t: String): (Long, String, Int) = {
        val values = t.split(" ")
        (values(0).toLong, values(1), values(2).toInt)
      }
    })

    val mapData = resultData.keyBy(0).flatMap(new RichFlatMapFunction[(Long, String, Int), (Long, String, Int)] {
      private var lastValueState: ValueState[Int] = _


      override def open(parameters: Configuration): Unit = {
        val descriptor = new ValueStateDescriptor[Int]("lastValue", classOf[Int])
        lastValueState = getRuntimeContext.getState(descriptor)
      }

      override def flatMap(in: (Long, String, Int), collector: Collector[(Long, String, Int)]): Unit = {
        println(in._2, "->")
        val lastValue = lastValueState.value()
        if(in._3 < lastValue) {
          collector.collect((in._1, in._2, lastValue))
        } else {
          lastValueState.update(in._3)
          collector.collect((in._1, in._2, in._3))
        }
      }
    })
    mapData.print()
//    val sums = mapData.keyBy(0).timeWindow(Time.seconds(5)).sum(2)
//    sums.print()

    env.execute("state test")
  }

}
