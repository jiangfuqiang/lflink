package table

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.scala._
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
    val projTable = tableEnv.scan("table1").join(table2).where('field1 === 'field3).select('field1,'field2,'field4)
//    val sinkTable = projTable.addSink(new SinkFunction[Row] {
//      override def invoke(value: Row, context: SinkFunction.Context[_]): Unit = {
//        println(row)
//      }
//    })



    val result = tableEnv.toAppendStream[Row](projTable).map(new MapFunction[Row, (Long, String, String)](){
      override def map(t: Row): (Long, String,String) = {
        (t.getField(0).toString.toLong,t.getField(1).toString, t.getField(2).toString)
      }
    })

    val groupResult = result.keyBy(0).timeWindow(Time.seconds(5)).sum(0)

    groupResult.print()

    env.execute("table test")

  }
}
