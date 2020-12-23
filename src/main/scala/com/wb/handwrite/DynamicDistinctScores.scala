package com.wb.handwrite

import java.net.URL

import org.apache.flink.streaming.api.scala._

/**
  * Descreption: XXXX<br/>
  * Date: 2020年12月23日
  *
  * @author WangBo
  * @version 1.0
  */
object DynamicDistinctScores {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputPathUrl: URL = this.getClass.getResource("/user_behavior.txt")
    val inputDataStream: DataStream[String] = env.readTextFile(inputPathUrl.getPath)

    val dataStream: DataStream[UserAction] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        UserAction(arr(0), arr(1).toInt, arr(2))
      })

  }
}
