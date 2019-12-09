package table

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableTest {
  def main(args: Array[String]): Unit = {



    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val tableEnv = StreamTableEnvironment.create(env)
    val inputData: DataStream[String] = env.socketTextStream("localhost", 9090)
    val inputData1: DataStream[String] = env.socketTextStream("localhost", 9091)
    val inputDataResult: DataStream[(Long, String)] = inputData.map(new MapFunction[String, (Long, String)] {
      override def map(t: String): (Long, String) = {
        val values = t.split(" ")
        (values(0).toLong, values(1))
      }
    })

    val inputDataResult1: DataStream[(Long, String)] = inputData1.map(new MapFunction[String, (Long, String)] {
      override def map(t: String): (Long, String) = {
        val values = t.split(" ")
        (values(0).toLong, values(1))
      }
    })
    tableEnv.registerDataStream("table1", inputDataResult, 'field1, 'field2)
    val table2 = tableEnv.fromDataStream(inputDataResult1, 'field3, 'field4)

    val projTable = tableEnv.scan("table1").leftOuterJoin(table2, "field1 = field3").select('field1,'field2,'field4)


    val tableEnvResult: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](projTable)

    val result = tableEnvResult.map(new MapFunction[(Boolean,Row), (Long, String, String)](){
      override def map(t: (Boolean,Row)): (Long, String,String) = {
        val row = t._2
        val value3Object = row.getField(2)
        var value3 = ""
        if(value3Object != null) {
          value3 = value3Object.toString
        }
        (row.getField(0).toString.toLong,row.getField(1).toString, value3)
      }
    })

    val groupResult = result.keyBy(0).flatMap(new RichFlatMapFunction[(Long, String, String),(Long, String, String)] {

      private var valueState: ValueState[(Long, String, String)] = _
        override def open(parameters: Configuration): Unit = {
          val stateDescriptor = new ValueStateDescriptor[(Long,String,String)]("tableTest",classOf[(Long, String, String)])
          valueState = getRuntimeContext.getState(stateDescriptor)
        }

        override def flatMap(in: (Long, String, String), collector: Collector[(Long, String, String)]): Unit = {
          val value = valueState.value()
          println("value -> ", value)
          if(value != null && value._1 == in._1) {
            collector.collect((value._1, value._2, in._3))
          } else {
            collector.collect(in)
          }
          valueState.update(in)
      }
    })
//      .keyBy(0).timeWindow(Time.seconds(5), Time.seconds(3)).sum(0)

    groupResult.print()

    env.execute("table test")

  }

  case class Person(val id: Long, val name1: String, val name2: String) {

  }
}


