package com.wb.mytest

import com.wb.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row


/**
  * Descreption:
  * 开一个滚动窗口，统计 10 秒内出现的每个 sensor 的个数。
  *
  * Date: 2020年06月29日
  *
  * @author WangBo
  * @version 1.0
  */
object TableWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据，获得 Stream
    val inputPathUrl = this.getClass.getResource("/sensor.txt")
    val inputStream: DataStream[String] = env.readTextFile(inputPathUrl.getPath)
    val sensorStream: DataStream[SensorReading] = inputStream.map(line => {
      val record: Array[String] = line.split(",")
      SensorReading(record(0), record(1).toLong, record(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    // 0.创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 1.将 Stream 转换为表
    val sensorTable: Table = tableEnv.fromDataStream(sensorStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    // 2. 表的查询操作
    val resultTable: Table = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'w)
      .groupBy('w, 'id)
      .select('id, 'w.start.toTimestamp, 'temperature.count as 'cnt)

    resultTable.toRetractStream[Row].print("sensor count")

    env.execute("table api example")

  }
}
