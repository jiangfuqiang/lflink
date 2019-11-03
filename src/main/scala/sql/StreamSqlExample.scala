package sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row

object StreamSqlExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val orderA: DataStream[Order] = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
    Order(3L, "rubber", 2)
    ))

    val orderB: DataStream[Order] = env.fromCollection(Seq(
      Order(2L, "pen",3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)
    ))


    var tableA = tEnv.fromDataStream(orderA, 'user, 'product,'amount)
    var tableB = tEnv.fromDataStream(orderB, 'user, 'product, 'amount)

    val result = tEnv.sqlQuery(s"SELECT * FROM $tableA where amount > 2").unionAll(tableB).where(" amount > 2")

    val dsRow:DataStream[Row] = tEnv.toAppendStream[Row](result)
    dsRow.print()
    env.execute()

  }

}

case class Order(user: Long, product: String, amount: Int) {

  override def toString: String = {
    return "Order{"+ "user=" + user + ", product=" + product + "\'" + ", amount=" + amount + "}"
  }
}
