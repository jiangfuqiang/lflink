package watermark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger

object WatermarkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)
    val inputData = env.socketTextStream("localhost", 9090)
        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[String] {
          var currentTimeStamp = 0L;
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimeStamp - 2000)
      }

      override def extractTimestamp(t: String, l: Long): Long = {
        val watermark = t.split(" ")(1).toLong
        currentTimeStamp = Math.max(watermark, currentTimeStamp)
        if(currentTimeStamp > 0) {

          println(sdf.format(new Date(currentTimeStamp)))
        }
        watermark
      }
    })

    val counts = inputData.filter(_.nonEmpty).map((m: String) => (m.split(" ")(0), 1))
      .keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5))
//      .allowedLateness(Time.seconds(1))
      .sum(1)
    counts.print()

    env.execute("EventTime processing example")
  }

}
