package sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TestSqlTimeWindow {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val stream:DataStream[(String, String)] = env.fromElements(("1","jiang"),
      ("2", "zhou"))

    val table = tEnv.fromDataStream(stream, 'id, 'username)

    val result: Table = tEnv.sqlQuery(s"select id, count(id) as cnt from $table group by id")

    val rowData:DataStream[Row] = tEnv.toAppendStream[Row](result)
    rowData.print()

    env.execute()

  }

}
