package com.wb.apitest.tabletest

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

/**
  * Descreption: XXXX<br/>
  * Date: 2020年06月28日
  *
  * @author WangBo
  * @version 1.0
  */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    // 0. 创建流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

//    // 1.1 老版本planner的流式查询
//    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
//      .useOldPlanner() // 用老版本
//      .inStreamingMode() // 用流处理
//      .build()
//
//    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

//     1.2 老版本批处理环境
//    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//    val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)

//    // 1.3 blink版本的流式查询
//    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
//      .useBlinkPlanner()
//      .inStreamingMode()
//      .build()
//
//    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

//    // 1.4 blink版本的批式查询
//    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
//      .useBlinkPlanner()
//      .inBatchMode()
//      .build()
//    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    // 2. 连接外部系统，读取数据
    // 2.1 读取文件数据
    val inputPathUrl = this.getClass.getResource("/sensor.txt")
    val filePath = inputPathUrl.getPath
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv()) // 定义从外部文件读取数据之后的格式化方法
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("inputTable") // 在表环境注册一张表

    // 测试输出
    val inputTable: Table = tableEnv.from("inputTable")
    inputTable.toAppendStream[(String, Long, Double)].print()


    env.execute("table api example")
    
  }
}
