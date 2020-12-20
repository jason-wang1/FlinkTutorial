package com.wb.apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月28日
  *
  * @author WangBo
  * @version 1.0
  */
object TableTest {
  def main(args: Array[String]): Unit = {
    // 0. 创建流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读入数据
    val inputStream = env.readTextFile("C:\\Users\\BoWANG\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // Transform操作
    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble )
        }
      )


    // 1. 基于env创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2. 基于tableEnv，将流转换成表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 3. 转换操作，得到提取结果
//    // 3.1 调用tableAPI
//    val resultTable: Table = dataTable
//      .select("id, temperature")
//      .filter("id == 'sensor_1'")

    // 3.2 写sql实现转换
    tableEnv.registerTable("dataTable", dataTable)
    val resultTable: Table = tableEnv.sqlQuery(
      "select id, temperaturefrom dataTablewhere id = 'sensor_1'")

    // 4. 把表转换成流，打印输出
    val resultStream: DataStream[(String, Double)] = resultTable
      .toAppendStream[(String, Double)]
    resultStream.print()

    env.execute("table api example")

  }
}
